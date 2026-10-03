"""Independent runtime-first publication v2; no durable or live binding serialized."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from collections import OrderedDict
from contextlib import closing
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from ..runtime import RUNTIME_COMPILER
from ..runtime.inventory import combine_inventories, load_p2_inventory
from ..runtime.snapshot import RuntimeSnapshot
from .location_gate import _is_canonical_path, parse_repository

FORMAT = "sakurapool-publication-v2"
MAX_MANIFEST = 1 << 20
MAX_MAP_LINE = 16 << 10
MAX_MAP_BYTES = 256 << 20
SHA = re.compile(r"[0-9a-f]{64}\Z")
REV = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
KEYS = frozenset(
    (
        "format publication_format_version publication_id runtime_format_version "
        "runtime_compiler snapshot_id source_fingerprint rid_count object_count "
        "runtime_root remote_objects image_sha256 remote_objects_bytes remote_objects_sha256 "
        "image_sha256_bytes image_sha256_sha256 runtime_current_sha256 snapshot_sha256 "
        "runtime_bytes fetchable_object_count fetchable_rid_count durable_included "
        "durable_distribution binding_mode"
    ).split()
)


class PublicationCorrupt(ValueError):
    pass


def plain(path, directory=False):
    from ..fs_safety import plain_entry

    try:
        return plain_entry(path, directory=directory)
    except (OSError, ValueError) as exc:
        raise PublicationCorrupt("publication entry containment/type") from exc


def sha(path):
    h = hashlib.sha256()
    with plain(path).open("rb") as stream:
        for b in iter(lambda: stream.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def decode(raw):
    def pairs(items):
        d = {}
        for k, v in items:
            if k in d:
                raise PublicationCorrupt("duplicate JSON key")
            d[k] = v
        return d

    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except (ValueError, UnicodeError) as e:
        raise PublicationCorrupt("invalid JSON") from e


def bounded(path, cap=MAX_MANIFEST):
    with plain(path).open("rb") as stream:
        raw = stream.read(cap + 1)
    if len(raw) > cap:
        raise PublicationCorrupt("JSON hard cap")
    return decode(raw)


def readonly(path):
    path = plain(path)
    for suffix in ("-wal", "-shm", "-journal"):
        side = Path(str(path) + suffix)
        if side.exists() or side.is_symlink():
            raise PublicationCorrupt("mutable SQLite sidecar forbidden")
    db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    db.execute("PRAGMA query_only=ON")
    db.execute("PRAGMA trusted_schema=OFF")
    return db


def map_rows(path):
    total = 0
    with plain(path).open("rb") as stream:
        while True:
            raw = stream.readline(MAX_MAP_LINE + 1)
            if not raw:
                break
            total += len(raw)
            if len(raw) > MAX_MAP_LINE or total > MAX_MAP_BYTES:
                raise PublicationCorrupt("remote map hard cap")
            r = decode(raw)
            if not isinstance(r, dict) or set(r) != set(
                (
                    "dataset_id endpoint repo_id repo_type revision_candidate "
                    "object_path object_size provider_sha256"
                ).split()
            ):
                raise PublicationCorrupt("map keys")
            parse_repository(r["repo_id"])
            if (
                r["endpoint"] != "https://modelscope.cn"
                or r["repo_type"] != "modelscope_dataset_legacy"
            ):
                raise PublicationCorrupt("unsupported locator")
            if (
                not isinstance(r["dataset_id"], str)
                or not r["dataset_id"]
                or not isinstance(r["revision_candidate"], str)
                or not REV.fullmatch(r["revision_candidate"])
                or not isinstance(r["object_path"], str)
                or not _is_canonical_path(r["object_path"])
            ):
                raise PublicationCorrupt("map identity")
            if type(r["object_size"]) is not int or not 0 <= r["object_size"] < 2**64:
                raise PublicationCorrupt("map size")
            if r["provider_sha256"] is not None and (
                not isinstance(r["provider_sha256"], str) or not SHA.fullmatch(r["provider_sha256"])
            ):
                raise PublicationCorrupt("provider digest")
            yield r


def p2_inputs(path):
    d = bounded(path, 4 << 20)
    if (
        not isinstance(d, dict)
        or set(d) != {"format", "roots"}
        or d["format"] != "sakurapool-p2-root-list-v1"
        or not isinstance(d["roots"], list)
        or not d["roots"]
    ):
        raise PublicationCorrupt("P2 list")
    roots = []
    for raw in d["roots"]:
        if not isinstance(raw, str) or not Path(raw).is_absolute() or ".." in Path(raw).parts:
            raise PublicationCorrupt("noncanonical P2 root")
        p = plain(raw, True)
        if p in roots:
            raise PublicationCorrupt("duplicate P2 root")
        roots.append(p)
    return combine_inventories([load_p2_inventory(p) for p in roots])


SCHEMA = """
CREATE TABLE meta(publication_format_version INTEGER NOT NULL,snapshot_id TEXT NOT NULL,
source_fingerprint TEXT NOT NULL,rid_count INTEGER NOT NULL,object_count INTEGER NOT NULL);
CREATE TABLE repositories(repo_idx INTEGER PRIMARY KEY,endpoint TEXT NOT NULL,
repo_id TEXT NOT NULL,repo_type TEXT NOT NULL,UNIQUE(endpoint,repo_id,repo_type));
CREATE TABLE objects(object_idx INTEGER PRIMARY KEY,dataset_id TEXT NOT NULL,
object_id TEXT NOT NULL,repo_idx INTEGER NOT NULL,revision_candidate TEXT NOT NULL,
object_path TEXT NOT NULL,object_size BLOB NOT NULL,content_sha256 BLOB NOT NULL,
provider_sha256 BLOB,fetchable INTEGER NOT NULL,UNIQUE(dataset_id,object_id));"""


def validate_schema(db):
    expected_tables = {"meta", "repositories", "objects"}
    entries = db.execute("SELECT type,name FROM sqlite_master LIMIT 33").fetchall()
    if (
        len(entries) > 32
        or {name for kind, name in entries if kind == "table"} != expected_tables
        or any(kind in ("view", "trigger") for kind, name in entries)
    ):
        raise PublicationCorrupt("catalog schema entries")
    with closing(sqlite3.connect(":memory:")) as reference:
        reference.executescript(SCHEMA)
        for table in expected_tables:
            if (
                db.execute("PRAGMA table_info(" + table + ")").fetchmany(33)
                != reference.execute("PRAGMA table_info(" + table + ")").fetchall()
            ):
                raise PublicationCorrupt("catalog schema columns")


def fetchable_rids(db, locations, object_count):
    """Object-sized state, bounded location blocks; never allocate by an untrusted max idx."""
    flags = np.zeros(object_count, dtype=np.uint8)
    seen = 0
    for idx, fetchable in db.execute(
        "SELECT object_idx,fetchable FROM objects ORDER BY object_idx"
    ):
        if type(idx) is not int or idx != seen or fetchable not in (0, 1):
            raise PublicationCorrupt("object index/fetchability")
        flags[idx] = fetchable
        seen += 1
    if seen != object_count:
        raise PublicationCorrupt("object index coverage")
    total = 0
    for start in range(0, len(locations), 4096):
        indexes = locations[start : start + 4096]["object_idx"]
        if len(indexes) and (indexes.min() < 0 or indexes.max() >= object_count):
            raise PublicationCorrupt("location object index")
        total += int(flags[indexes.astype(np.intp)].sum())
    return total


def build_publication(runtime, p2_list, remote_map, output):
    runtime = plain(runtime, True)
    output = Path(output).absolute()
    plain(output.parent, True)
    if output.exists() or output.is_relative_to(runtime) or runtime.is_relative_to(output):
        raise PublicationCorrupt("fresh nonoverlapping output required")
    inv = p2_inputs(p2_list)
    current = bounded(runtime / "current.json")
    sid = current.get("snapshot_id") if isinstance(current, dict) else None
    if (
        not isinstance(sid, str)
        or not SHA.fullmatch(sid)
        or current.get("path") != "snapshots/" + sid
    ):
        raise PublicationCorrupt("source runtime containment")
    pinned = plain(runtime / "snapshots" / sid, True)
    source_manifest = bounded(pinned / "SNAPSHOT.json")
    plain(pinned / "READY")
    for entry in source_manifest["files"].values():
        if not isinstance(entry["path"], str) or Path(entry["path"]).name != entry["path"]:
            raise PublicationCorrupt("source runtime file containment")
        plain(pinned / entry["path"])
    with RuntimeSnapshot.open(runtime, full_verify=True) as rt:
        if inv.source_fingerprint != rt.manifest["source_fingerprint"]:
            raise PublicationCorrupt("P2 fingerprint mismatch")
        sid, count = rt.snapshot_id, rt.rid_count
        from ..fs_safety import OwnedStage, cleanup_owned_tree, sync_directory

        stage = Path(tempfile.mkdtemp(prefix=".publication-", dir=output.parent))
        owned = OwnedStage(stage)
        try:
            dest = stage / "runtime"
            owned.create("runtime", directory=True)
            owned.create("runtime/snapshots", directory=True)
            owned.create(Path("runtime/snapshots") / sid, directory=True)
            for base, dirs, files in os.walk(rt.path, followlinks=False):
                for n in dirs:
                    plain(Path(base) / n, True)
                for n in files:
                    plain(Path(base) / n)
            shutil.copy2(runtime / "current.json", owned.create("runtime/current.json"))
            for base, dirs, files in os.walk(rt.path, followlinks=False):
                relative = Path("runtime/snapshots") / sid / Path(base).relative_to(rt.path)
                for name in dirs:
                    owned.create(relative / name, directory=True)
                for name in files:
                    shutil.copy2(Path(base) / name, owned.create(relative / name))
            cat = owned.create("remote_objects.sqlite")
            with closing(sqlite3.connect(cat)) as db:
                db.execute("PRAGMA journal_mode=OFF")
                db.executescript(SCHEMA)
                objects = rt._catalog.execute("SELECT count(*) FROM objects").fetchone()[0]
                db.execute(
                    "INSERT INTO meta VALUES(2,?,?,?,?)",
                    (sid, inv.source_fingerprint, count, objects),
                )
                db.execute(
                    "CREATE TEMP TABLE runtime_objects(dataset_id TEXT,object_path TEXT,"
                    "object_idx INTEGER,object_id TEXT,object_size BLOB,object_version TEXT,"
                    "PRIMARY KEY(dataset_id,object_path)) WITHOUT ROWID"
                )
                try:
                    db.executemany(
                        "INSERT INTO runtime_objects VALUES(?,?,?,?,?,?)",
                        rt._catalog.execute(
                            "SELECT dataset_id,object_path,object_idx,object_id,"
                            "object_size,object_version FROM objects"
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise PublicationCorrupt("duplicate runtime object path") from exc
                for r in map_rows(remote_map):
                    found = db.execute(
                        "SELECT object_idx,object_id,object_size,object_version "
                        "FROM runtime_objects "
                        "WHERE dataset_id=? AND object_path=?",
                        (r["dataset_id"], r["object_path"]),
                    ).fetchall()
                    if len(found) != 1 or found[0][2] != r["object_size"].to_bytes(8, "big"):
                        raise PublicationCorrupt("map exact coverage mismatch")
                    idx, oid, _, content = found[0]
                    if not isinstance(content, str) or not SHA.fullmatch(content):
                        raise PublicationCorrupt("indexed whole SHA absent")
                    provider = r["provider_sha256"]
                    if provider is not None and provider != content:
                        raise PublicationCorrupt("provider SHA mismatch")
                    db.execute(
                        "INSERT OR IGNORE INTO repositories(endpoint,repo_id,repo_type) "
                        "VALUES(?,?,?)",
                        (r["endpoint"], r["repo_id"], r["repo_type"]),
                    )
                    repo = db.execute(
                        "SELECT repo_idx FROM repositories "
                        "WHERE endpoint=? AND repo_id=? AND repo_type=?",
                        (r["endpoint"], r["repo_id"], r["repo_type"]),
                    ).fetchone()[0]
                    db.execute(
                        "INSERT INTO objects VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            idx,
                            r["dataset_id"],
                            oid,
                            repo,
                            r["revision_candidate"],
                            r["object_path"],
                            r["object_size"].to_bytes(8, "big"),
                            bytes.fromhex(content),
                            None if provider is None else bytes.fromhex(provider),
                            int(provider is not None),
                        ),
                    )
                if db.execute("SELECT count(*) FROM objects").fetchone()[0] != objects:
                    raise PublicationCorrupt("missing map object")
                db.commit()
            temp = owned.create("hashes.sqlite")
            arr = None
            with closing(sqlite3.connect(temp, uri=True)) as db:
                db.execute("PRAGMA journal_mode=OFF")
                db.execute("PRAGMA cache_size=-8192")
                db.execute(
                    "CREATE TABLE hashes(record_id BLOB PRIMARY KEY,image_sha BLOB NOT NULL) "
                    "WITHOUT ROWID"
                )
                for obj in inv.objects:
                    for frag in obj.fragments:
                        if frag.name != "samples":
                            continue
                        for batch in pq.ParquetFile(frag.path).iter_batches(
                            batch_size=4096,
                            columns=["record_id", "hash_source", "hash_kind", "sha256"],
                        ):
                            for r in batch.to_pylist():
                                if (
                                    r["hash_source"] != "computed:sha256"
                                    or r["hash_kind"] != "sha256"
                                    or not isinstance(r["sha256"], str)
                                    or not SHA.fullmatch(r["sha256"])
                                ):
                                    raise PublicationCorrupt("reliable image SHA required")
                            db.executemany(
                                "INSERT INTO hashes VALUES(?,?)",
                                [
                                    (bytes.fromhex(r["record_id"]), bytes.fromhex(r["sha256"]))
                                    for r in batch.to_pylist()
                                ],
                            )
                db.commit()
                if db.execute("SELECT count(*) FROM hashes").fetchone()[0] != count:
                    raise PublicationCorrupt("hash count mismatch")
                hash_path = owned.create("image_sha256.npy")
                arr = np.lib.format.open_memmap(hash_path, mode="w+", dtype="V32", shape=(count,))
                seen = 0
                db.execute(
                    "ATTACH DATABASE ? AS runtime",
                    (rt.path.joinpath("catalog.sqlite").as_uri() + "?mode=ro&immutable=1",),
                )
                for rid, image_sha in db.execute(
                    "SELECT r.rid,h.image_sha FROM runtime.records AS r "
                    "LEFT JOIN hashes AS h ON h.record_id=r.record_id ORDER BY r.rid"
                ):
                    if rid != seen or image_sha is None:
                        raise PublicationCorrupt("missing hash/noncontiguous rid")
                    arr[rid] = np.void(image_sha)
                    seen += 1
                if seen != count:
                    raise PublicationCorrupt("runtime record count")
                arr.flush()
                arr._mmap.close()
                arr = None
            owned.remove("hashes.sqlite")
            with closing(readonly(cat)) as db:
                fc = db.execute("SELECT count(*) FROM objects WHERE fetchable=1").fetchone()[0]
                fr = fetchable_rids(db, rt._locations, objects)
            m = dict(
                format=FORMAT,
                publication_format_version=2,
                publication_id=sid,
                runtime_format_version=2,
                runtime_compiler=RUNTIME_COMPILER,
                snapshot_id=sid,
                source_fingerprint=inv.source_fingerprint,
                rid_count=count,
                object_count=objects,
                runtime_root="runtime",
                remote_objects="remote_objects.sqlite",
                image_sha256="image_sha256.npy",
                remote_objects_bytes=cat.stat().st_size,
                remote_objects_sha256=sha(cat),
                image_sha256_bytes=(stage / "image_sha256.npy").stat().st_size,
                image_sha256_sha256=sha(stage / "image_sha256.npy"),
                runtime_current_sha256=sha(dest / "current.json"),
                snapshot_sha256=sha(dest / "snapshots" / sid / "SNAPSHOT.json"),
                runtime_bytes=sum(p.stat().st_size for p in dest.rglob("*") if p.is_file()),
                fetchable_object_count=fc,
                fetchable_rid_count=fr,
                durable_included=False,
                durable_distribution="separate",
                binding_mode="fresh_per_object_strong_etag_if_match",
            )
            raw = json.dumps(m, sort_keys=True).encode()
            if len(raw) > MAX_MANIFEST:
                raise PublicationCorrupt("manifest cap")
            owned.create("PUBLICATION.json").write_bytes(raw)
            owned.create("READY").write_text(hashlib.sha256(raw).hexdigest(), encoding="ascii")
            owned.complete()
            with load_publication(stage, full_verify=True):
                pass
            from .retrieval import _publish_directory

            owned.complete()
            for relative, (_, _, directory) in owned.entries.items():
                file = stage / relative
                if not directory:
                    with file.open("r+b") as stream:
                        os.fsync(stream.fileno())
            for directory in sorted(
                (p for p in stage.rglob("*") if p.is_dir()),
                key=lambda p: len(p.parts),
                reverse=True,
            ):
                sync_directory(directory)
            sync_directory(stage)
            owned.complete()
            _publish_directory(stage, output)
            sync_directory(output.parent)
            return m
        except Exception:
            if "arr" in locals() and arr is not None:
                try:
                    arr._mmap.close()
                except Exception:
                    pass
            cleanup_owned_tree(stage, owned.entries)
            raise


class Publication:
    def __init__(self, root, m, rt, db, arr):
        self.root, self.manifest, self.runtime, self.catalog, self.hashes = root, m, rt, db, arr
        self._verified = OrderedDict()
        self._closed = False
        self.full_verified = False

    def expected_image_sha(self, rid):
        if self._closed or type(rid) is not int or not 0 <= rid < self.runtime.rid_count:
            raise PublicationCorrupt("publication rid/lifecycle")
        return self.hashes[rid].tobytes()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._verified.clear()
        self.runtime.close()
        self.catalog.close()
        self.hashes._mmap.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def inspect(self):
        return {
            k: self.manifest[k]
            for k in (
                "publication_id",
                "snapshot_id",
                "rid_count",
                "object_count",
                "fetchable_object_count",
                "fetchable_rid_count",
                "runtime_bytes",
                "image_sha256_bytes",
                "remote_objects_bytes",
            )
        } | {"p2_format_lineage": 4, "runtime_format": 2}


def load_publication(root, full_verify=False):
    root = plain(root, True)
    with plain(root / "PUBLICATION.json").open("rb") as stream:
        manifest_raw = stream.read(MAX_MANIFEST + 1)
    if len(manifest_raw) > MAX_MANIFEST:
        raise PublicationCorrupt("JSON hard cap")
    content_digest = hashlib.sha256(manifest_raw).hexdigest()
    m = decode(manifest_raw)
    if (
        not isinstance(m, dict)
        or set(m) != KEYS
        or m["format"] != FORMAT
        or m["publication_format_version"] != 2
        or m["runtime_format_version"] != 2
        or m["runtime_compiler"] != RUNTIME_COMPILER
        or m["durable_included"] is not False
        or m["durable_distribution"] != "separate"
        or m["binding_mode"] != "fresh_per_object_strong_etag_if_match"
    ):
        raise PublicationCorrupt("manifest contract")
    for k in (
        "publication_id",
        "snapshot_id",
        "source_fingerprint",
        "remote_objects_sha256",
        "image_sha256_sha256",
        "runtime_current_sha256",
        "snapshot_sha256",
    ):
        if not isinstance(m[k], str) or not SHA.fullmatch(m[k]):
            raise PublicationCorrupt("manifest identity")
    for k in (
        "rid_count",
        "object_count",
        "remote_objects_bytes",
        "image_sha256_bytes",
        "runtime_bytes",
        "fetchable_object_count",
        "fetchable_rid_count",
    ):
        if type(m[k]) is not int or m[k] < 0:
            raise PublicationCorrupt("manifest count")
    if (m["runtime_root"], m["remote_objects"], m["image_sha256"]) != (
        "runtime",
        "remote_objects.sqlite",
        "image_sha256.npy",
    ):
        raise PublicationCorrupt("manifest paths")
    with plain(root / "READY").open("rb") as stream:
        publication_ready = stream.read(65)
    if publication_ready != content_digest.encode("ascii"):
        raise PublicationCorrupt("not READY")
    if m["publication_id"] != m["snapshot_id"]:
        raise PublicationCorrupt("publication identity mismatch")
    rr = plain(root / "runtime", True)
    current = bounded(rr / "current.json")
    if (
        not isinstance(current, dict)
        or current.get("path") != "snapshots/" + m["snapshot_id"]
        or current.get("snapshot_id") != m["snapshot_id"]
    ):
        raise PublicationCorrupt("runtime current containment")
    pinned = plain(rr / "snapshots" / m["snapshot_id"], True)
    snapshot = bounded(pinned / "SNAPSHOT.json")
    ready = plain(pinned / "READY")
    with ready.open("rb") as stream:
        ready_contents = stream.read(66)
    if ready_contents not in (m["snapshot_id"].encode(), (m["snapshot_id"] + "\n").encode()):
        raise PublicationCorrupt("runtime READY identity")
    for entry in snapshot["files"].values():
        if not isinstance(entry["path"], str) or Path(entry["path"]).name != entry["path"]:
            raise PublicationCorrupt("runtime file containment")
        plain(pinned / entry["path"])
    if (
        sha(rr / "current.json") != m["runtime_current_sha256"]
        or sha(rr / "snapshots" / m["snapshot_id"] / "SNAPSHOT.json") != m["snapshot_sha256"]
    ):
        raise PublicationCorrupt("runtime pin")
    rt = RuntimeSnapshot.open(rr, full_verify=full_verify)
    db = arr = None
    try:
        if (
            rt.snapshot_id != m["snapshot_id"]
            or rt.rid_count != m["rid_count"]
            or rt.manifest["source_fingerprint"] != m["source_fingerprint"]
        ):
            raise PublicationCorrupt("runtime identity")
        for name, key in [
            ("remote_objects.sqlite", "remote_objects"),
            ("image_sha256.npy", "image_sha256"),
        ]:
            p = plain(root / name)
            if p.stat().st_size != m[key + "_bytes"] or (
                full_verify and sha(p) != m[key + "_sha256"]
            ):
                raise PublicationCorrupt("sidecar size/hash")
        db = readonly(root / "remote_objects.sqlite")
        validate_schema(db)
        meta_rows = db.execute("SELECT * FROM meta LIMIT 2").fetchall()
        if meta_rows != [
            (2, rt.snapshot_id, m["source_fingerprint"], rt.rid_count, m["object_count"])
        ]:
            raise PublicationCorrupt("catalog meta")
        if (
            db.execute("SELECT count(*) FROM objects").fetchone()[0] != m["object_count"]
            or rt._catalog.execute("SELECT count(*) FROM objects").fetchone()[0]
            != m["object_count"]
        ):
            raise PublicationCorrupt("object count")
        arr = np.load(root / "image_sha256.npy", mmap_mode="r", allow_pickle=False)
        if arr.dtype != np.dtype("V32") or arr.shape != (rt.rid_count,):
            raise PublicationCorrupt("hash dtype/shape")
        if full_verify:
            for endpoint, repo, kind in db.execute(
                "SELECT endpoint,repo_id,repo_type FROM repositories"
            ):
                parse_repository(repo)
                if endpoint != "https://modelscope.cn" or kind != "modelscope_dataset_legacy":
                    raise PublicationCorrupt("catalog repository identity")
            runtime_bytes = (rr / "current.json").stat().st_size + sum(
                plain(pinned / name).stat().st_size
                for name in [
                    "READY",
                    "SNAPSHOT.json",
                    "OWNER.json",
                    "STAGE.txt",
                    *(e["path"] for e in snapshot["files"].values()),
                ]
            )
            if runtime_bytes != m["runtime_bytes"]:
                raise PublicationCorrupt("runtime bytes mismatch")
            expected_rid = 0
            for rid, record_id in rt._catalog.execute(
                "SELECT rid,record_id FROM records ORDER BY rid"
            ):
                if rid != expected_rid or not isinstance(record_id, bytes) or len(record_id) != 16:
                    raise PublicationCorrupt("runtime rid identity")
                expected_rid += 1
            if (
                expected_rid != rt.rid_count
                or rt._catalog.execute("SELECT count(DISTINCT record_id) FROM records").fetchone()[
                    0
                ]
                != rt.rid_count
            ):
                raise PublicationCorrupt("runtime record identity coverage")
            for idx, ds, oid, path, size, content in rt._catalog.execute(
                "SELECT object_idx,dataset_id,object_id,object_path,object_size,object_version "
                "FROM objects ORDER BY object_idx"
            ):
                r = db.execute(
                    "SELECT dataset_id,object_id,object_path,object_size,content_sha256,"
                    "provider_sha256,fetchable,revision_candidate,repo_idx "
                    "FROM objects WHERE object_idx=?",
                    (idx,),
                ).fetchone()
                if (
                    r is None
                    or r[:5] != (ds, oid, path, size, bytes.fromhex(content))
                    or r[5] not in (None, r[4])
                    or r[6] != int(r[5] is not None)
                    or not REV.fullmatch(r[7])
                    or db.execute(
                        "SELECT count(*) FROM repositories WHERE repo_idx=?", (r[8],)
                    ).fetchone()[0]
                    != 1
                ):
                    raise PublicationCorrupt("object exact mismatch")
            if (
                db.execute("SELECT count(*) FROM objects WHERE fetchable=1").fetchone()[0]
                != m["fetchable_object_count"]
            ):
                raise PublicationCorrupt("fetchable objects")
            n = fetchable_rids(db, rt._locations, m["object_count"])
            if n != m["fetchable_rid_count"]:
                raise PublicationCorrupt("fetchable rids")
        publication = Publication(root, m, rt, db, arr)
        publication.full_verified = full_verify
        publication.content_digest = content_digest
        return publication
    except Exception:
        rt.close()
        if db is not None:
            db.close()
        if arr is not None and hasattr(arr, "_mmap"):
            arr._mmap.close()
        raise
