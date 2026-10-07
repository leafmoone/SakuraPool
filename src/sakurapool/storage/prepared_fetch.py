"""Owner-created immutable fetch descriptors and constant-space extent plans."""

from collections import OrderedDict
from dataclasses import dataclass, field
from threading import get_ident
from types import MappingProxyType, SimpleNamespace

from ..capacity import UINT64_MAX, CapacityConfig
from .production_resources import SESSION_ATTEMPTS_PER_TRANSFER, chunked_body_budget
from .publication import PublicationCorrupt

_FACTORY = object()


@dataclass(frozen=True)
class StreamPlan:
    """Arithmetic only: planning never authorizes or performs remote IO."""

    image_offset: int
    image_bytes: int
    metadata_offset: int
    metadata_bytes: int
    chunk_bytes: int

    @property
    def image_chunks(self):
        return (self.image_bytes + self.chunk_bytes - 1) // self.chunk_bytes

    @property
    def metadata_chunks(self):
        return (self.metadata_bytes + self.chunk_bytes - 1) // self.chunk_bytes

    @property
    def chunk_count(self):
        return self.image_chunks + self.metadata_chunks

    @property
    def saved_bytes(self):
        return self.image_bytes + self.metadata_bytes

    @property
    def max_chunk(self):
        return min(self.chunk_bytes, max(self.image_bytes, self.metadata_bytes))

    @property
    def generation_body(self):
        return chunked_body_budget(self.image_bytes, self.metadata_bytes, self.chunk_bytes)

    @property
    def generation_attempts(self):
        return SESSION_ATTEMPTS_PER_TRANSFER * self.chunk_count

    def lengths(self):
        """Yield actual Range lengths without constructing a chunk-sized list."""
        for metadata in (False, True):
            for _, length in self.chunks(metadata=metadata):
                yield length

    def chunks(self, *, metadata=False):
        offset = self.metadata_offset if metadata else self.image_offset
        remaining = self.metadata_bytes if metadata else self.image_bytes
        while remaining:
            length = min(remaining, self.chunk_bytes)
            yield offset, length
            offset += length
            remaining -= length


def stream_plan(location, object_size, *, metadata=False, capacity=None):
    capacity = capacity if capacity is not None else CapacityConfig()
    if not isinstance(capacity, CapacityConfig):
        raise ValueError("typed capacity required")
    names = ("image_offset", "image_size", "metadata_offset", "metadata_size")
    if (type(object_size) is not int or not 0 < object_size <= UINT64_MAX
            or any(type(location[n]) is not int or not 0 <= location[n] <= UINT64_MAX
                   for n in names)):
        raise PublicationCorrupt("stream extent integer bound")
    image, meta = location["image_size"], location["metadata_size"]
    if (not 0 < image <= capacity.image_max_bytes
            or meta > capacity.metadata_max_bytes
            or location["image_offset"] + image > object_size
            or location["metadata_offset"] + meta > object_size):
        raise PublicationCorrupt("stream extent capacity or object bound")
    meta = meta if metadata and location["flags"] & 1 else 0
    if image + meta > UINT64_MAX:
        raise PublicationCorrupt("stream saved byte integer bound")
    return StreamPlan(location["image_offset"], image, location["metadata_offset"],
                      meta, capacity.range_chunk_bytes)


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

    @property
    def transport_identity(self):
        # Authority tuple is selected by the bounded SQL projection below.
        endpoint, repo_id, repo_type, revision, path, size = self.catalog_row[:6]
        return endpoint, repo_id, repo_type, revision, path, int.from_bytes(size, "big")

    @classmethod
    def _prepare(cls, pub, record_id, *, capacity=None, image_extensions=None):
        from .publication import Publication

        if not isinstance(pub, Publication) or pub._closed or not pub.full_verified:
            raise PublicationCorrupt("prepared fetch requires verified publication")
        rt = pub.runtime
        rid = rt.resolve_record(record_id).rid
        raw_loc = rt.location(rid)
        loc = {name: raw_loc[name] for name in ("object_idx", "image_offset", "image_size",
               "metadata_offset", "metadata_size", "format_id", "flags")}
        image_format = rt.image_format(loc["format_id"])
        from ..image_formats import image_filename

        try:
            image_filename(image_format, image_extensions)
        except ValueError:
            raise PublicationCorrupt("prepared extent or format invalid") from None
        if len(record_id) != 32:
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
        if any(type(loc[name]) is not int or not 0 <= loc[name] < 1 << 64 for name in loc):
            raise PublicationCorrupt("prepared location bound")
        if any(type(value) is not str or len(value) > 2048 for value in row[:5]):
            raise PublicationCorrupt("prepared identity text bound")
        size = int.from_bytes(row[5], "big")
        stream_plan(loc, size, metadata=True, capacity=capacity)
        raw_ref = rt._prepared_object_ref(loc["object_idx"])
        object_ref = {name: raw_ref[name] for name in
                      ("object_path", "object_size", "object_version")}
        if (type(object_ref["object_path"]) is not str
                or len(object_ref["object_path"]) > 2048
                or type(object_ref["object_size"]) is not int
                or not 0 <= object_ref["object_size"] < 1 << 64
                or type(object_ref["object_version"]) is not str
                or len(object_ref["object_version"]) != 64):
            raise PublicationCorrupt("prepared object reference bound")
        if (object_ref["object_path"] != row[4] or object_ref["object_size"] != size
                or object_ref["object_version"] != row[6].hex()):
            raise PublicationCorrupt("prepared object identity mismatch")
        return cls(pub.content_digest, rt.snapshot_id, record_id, rid,
                   MappingProxyType(loc), MappingProxyType(object_ref),
                   tuple(row), pub.expected_image_sha(rid), image_format, _FACTORY, get_ident())

    def plan(self, *, metadata=False, capacity=None):
        return stream_plan(self.location, self.object_ref["object_size"],
                           metadata=metadata, capacity=capacity)

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
