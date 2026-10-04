"""Immutable single-record projection prepared only by the publication owner."""

from collections import OrderedDict
from dataclasses import dataclass, field
from threading import get_ident
from types import MappingProxyType, SimpleNamespace

from .publication import PublicationCorrupt

_FACTORY = object()


@dataclass(frozen=True)
class PreparedFetch:
    content_digest: str
    snapshot_id: str
    record_id: str
    rid: int
    location: object
    object_ref: object
    catalog_row: tuple
    image_sha: bytes
    image_format: str
    _provenance: object = field(repr=False)
    _owner: int = field(repr=False)

    def __post_init__(self):
        if self._provenance is not _FACTORY or self._owner != get_ident():
            raise PublicationCorrupt("prepared descriptor must be owner-created")

    @classmethod
    def _prepare(cls, pub, record_id):
        from .publication import Publication

        if not isinstance(pub, Publication) or pub._closed or not pub.full_verified:
            raise PublicationCorrupt("prepared fetch requires verified publication")
        rt = pub.runtime
        rid = rt.resolve_record(record_id).rid
        raw_loc = rt.location(rid)
        loc = {name: raw_loc[name] for name in ("object_idx", "image_offset", "image_size",
               "metadata_offset", "metadata_size", "format_id", "flags")}
        del raw_loc
        image_format = rt.image_format(loc["format_id"])
        if (len(record_id) != 32 or not 0 < loc["image_size"] <= 8 << 20
                or loc["metadata_size"] > 8 << 20
                or image_format not in ("jpg", "jpeg", "png", "webp", "avif")):
            raise PublicationCorrupt("prepared extent or format invalid")
        row = pub.catalog.execute(
            "SELECT r.endpoint,r.repo_id,r.repo_type,o.revision_candidate,o.object_path,"
            "o.object_size,o.content_sha256,o.provider_sha256,o.fetchable "
            "FROM objects o JOIN repositories r USING(repo_idx) WHERE object_idx=? "
            "AND typeof(r.endpoint)='text' AND length(r.endpoint)<=2048 "
            "AND instr(r.endpoint,char(0))=0 "
            "AND typeof(r.repo_id)='text' AND length(r.repo_id)<=2048 "
            "AND instr(r.repo_id,char(0))=0 "
            "AND typeof(r.repo_type)='text' AND length(r.repo_type)<=2048 "
            "AND instr(r.repo_type,char(0))=0 "
            "AND typeof(o.revision_candidate)='text' AND length(o.revision_candidate)<=2048 "
            "AND instr(o.revision_candidate,char(0))=0 "
            "AND typeof(o.object_path)='text' AND length(o.object_path)<=2048 "
            "AND instr(o.object_path,char(0))=0 "
            "AND typeof(o.object_size)='blob' AND length(o.object_size)=8 "
            "AND typeof(o.content_sha256)='blob' AND length(o.content_sha256)=32 "
            "AND typeof(o.provider_sha256)='blob' AND length(o.provider_sha256)=32",
            (loc["object_idx"],)).fetchone()
        if (row is None or row[8] != 1 or row[7] != row[6]
                or not isinstance(row[7], bytes) or len(row[7]) != 32):
            raise PublicationCorrupt("prepared object unavailable")
        if any(type(loc[name]) is not int or not 0 <= loc[name] < 1 << 64
               for name in ("object_idx", "image_offset", "image_size", "metadata_offset",
                            "metadata_size", "format_id", "flags")):
            raise PublicationCorrupt("prepared location bound")
        if any(type(value) is not str or len(value) > 2048 for value in row[:5]):
            raise PublicationCorrupt("prepared identity text bound")
        size = int.from_bytes(row[5], "big")
        raw_ref = rt._prepared_object_ref(loc["object_idx"])
        object_ref = {name: raw_ref[name] for name in
                      ("object_path", "object_size", "object_version")}
        del raw_ref
        if (type(object_ref["object_path"]) is not str
                or len(object_ref["object_path"]) > 2048
                or type(object_ref["object_size"]) is not int
                or not 0 <= object_ref["object_size"] < 1 << 64
                or type(object_ref["object_version"]) is not str
                or len(object_ref["object_version"]) != 64):
            raise PublicationCorrupt("prepared object reference bound")
        if (loc["image_offset"] + loc["image_size"] > size
                or loc["metadata_offset"] + loc["metadata_size"] > size
                or object_ref["object_path"] != row[4]
                or object_ref["object_size"] != size
                or object_ref["object_version"] != row[6].hex()):
            raise PublicationCorrupt("prepared object identity mismatch")
        return cls(pub.content_digest, rt.snapshot_id, record_id, rid,
                   MappingProxyType(loc), MappingProxyType(object_ref),
                   tuple(row), pub.expected_image_sha(rid), image_format, _FACTORY, get_ident())

    def _projection(self, cache):
        if self._provenance is not _FACTORY:
            raise PublicationCorrupt("prepared descriptor provenance invalid")
        prepared = self

        class Runtime:
            snapshot_id = prepared.snapshot_id

            def resolve_record(self, identity):
                if identity != prepared.record_id:
                    raise PublicationCorrupt("prepared identity mismatch")
                return SimpleNamespace(rid=prepared.rid)

            def location(self, rid):
                if rid != prepared.rid:
                    raise PublicationCorrupt("prepared rid mismatch")
                return dict(prepared.location)

            def object_ref(self, index):
                if index != prepared.location["object_idx"]:
                    raise PublicationCorrupt("prepared object mismatch")
                return dict(prepared.object_ref)

            def image_format(self, index):
                if index != prepared.location["format_id"]:
                    raise PublicationCorrupt("prepared format mismatch")
                return prepared.image_format

        class Catalog:
            def execute(self, sql, parameters):
                if parameters != (prepared.location["object_idx"],):
                    raise PublicationCorrupt("prepared catalog mismatch")
                return SimpleNamespace(fetchone=lambda: prepared.catalog_row)

        return SimpleNamespace(_closed=False, full_verified=True, runtime=Runtime(),
                               catalog=Catalog(), content_digest=self.content_digest,
                               _verified=cache if cache is not None else OrderedDict(),
                               expected_image_sha=lambda rid:
                               self.image_sha if rid == self.rid else None)
