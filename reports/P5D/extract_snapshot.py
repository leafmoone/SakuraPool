"""Safe streaming fixed snapshot extraction, using existing P2 member hashes."""
import hashlib
import json
import re
import stat
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path("D:/SakuraTool/SakuraPool-P5D-20261004T143905Z")
BASE = ROOT / "download/snapshots/20261004T143905Z-upload"
RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
            *(f"LPT{i}" for i in range(1, 10))}


def safe_parts(name, machine):
    if "\\" in name or ":" in name or name.startswith("/"):
        raise ValueError("unsafe archive path")
    parts = PurePosixPath(name).parts
    if not parts or parts[0] != machine or name != "/".join(parts):
        raise ValueError("unexpected archive prefix/normalization")
    for part in parts:
        if (part in (".", "..") or part.endswith((".", " "))
                or re.search(r'[<>"|?*\x00-\x1f]', part)
                or part.split(".")[0].upper() in RESERVED):
            raise ValueError("unsafe Windows archive component")
    return parts


def safe_directory(path):
    """Walk ancestors before mkdir; reject symlink and every Windows reparse type.

    Requires exclusive ownership: lstat/open are not race-proof against an attacker.
    """
    path = Path(path).absolute()
    for directory in (*reversed(path.parents), path):
        try:
            info = directory.lstat()
        except FileNotFoundError:
            directory.mkdir()
            info = directory.lstat()
        if (stat.S_ISLNK(info.st_mode)
                or getattr(info, "st_file_attributes", 0) & 0x400
                or not stat.S_ISDIR(info.st_mode)):
            raise ValueError("unsafe directory/reparse ancestor")


def extract(machine):
    package = json.loads((BASE / machine / f"{machine}-PACKAGE.json").read_bytes())
    verified = json.loads((BASE / machine / f"{machine}-PACKAGE-VERIFIED.json").read_bytes())
    archive = BASE / machine / f"{machine}-p2-snapshot.tar.gz"
    assert archive.stat().st_size == package["archive_bytes"] == verified["archive_bytes"]
    assert package["archive_sha256"] == verified["archive_sha256"]
    target = ROOT / "extracted"
    safe_directory(target)
    assert not (target / machine).exists()
    seen, expected, actual = set(), None, set()
    count = p2_bytes = p2_count = total = 0
    with tarfile.open(archive, "r|gz") as tar:
        for member in tar:
            parts = safe_parts(member.name, machine)
            key = "/".join(parts).casefold()
            if key in seen or not member.isfile() or member.size < 0:
                raise ValueError("duplicate or nonregular member")
            seen.add(key)
            count += 1
            total += member.size
            if (count > verified["archive_members"]
                    or total > package["totals"]["durable_bytes"] + (64 << 20)):
                raise ValueError("archive bound")
            path = target.joinpath(*parts)
            safe_directory(path.parent)
            try:
                path.lstat()
            except FileNotFoundError:
                pass
            else:
                raise ValueError("existing destination including link/reparse")
            stream = tar.extractfile(member)
            digest = hashlib.sha256()
            with stream, path.open("xb") as output:
                left = member.size
                while left:
                    data = stream.read(min(1 << 20, left))
                    if not data:
                        raise ValueError("truncated member")
                    output.write(data)
                    digest.update(data)
                    left -= len(data)
            if count == 1:
                assert member.name == machine + "/SNAPSHOT_REPORT.json"
                report = json.loads(path.read_bytes())
                assert report["machine_id"] == machine
                assert report["code_commit"] == "57864e5dd456251b457238b196e8fed5668d909b"
                assert report["original_checkpoint_103_included"] is False
                expected = {machine + "/" + row["path"]: row for row in report["p2_members"]}
                assert len(expected) == package["p2_files"]
            if member.name in expected:
                row = expected[member.name]
                assert member.size == row["bytes"] and digest.hexdigest() == row["sha256"]
                actual.add(member.name)
                p2_count += 1
                p2_bytes += member.size
            elif len(parts) > 1 and parts[1] == "p2":
                raise ValueError("undeclared P2 member")
    assert count == verified["archive_members"] and actual == set(expected)
    assert p2_count == package["p2_files"] == verified["p2_members"]
    assert p2_bytes == package["totals"]["durable_bytes"]
    print(json.dumps({"machine": machine, "members": count, "p2_files": p2_count,
                      "p2_bytes": p2_bytes, "all_extracted_bytes": total,
                      "manifest_integrity": "PASS"}), flush=True)


if __name__ == "__main__":
    for machine in ("11311", "12435", "12436"):
        extract(machine)
