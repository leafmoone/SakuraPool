"""Small, read-only bridge to audited modelscope-hub 0.4.0 dataset URLs.

The official SDK's LegacyClient has hidden HTTPAdapter retries (default 5),
redirects and a paginated API call loop. Calling it would bypass the single
persistent P4 transport quota. The reviewed SDK source is *only* the reference
for these two public legacy GET routes; this provider performs zero HTTP itself.
Unknown response shapes, truncated pagination and floating revisions fail
closed. No download_repo/snapshot_download, SDK cache, or upload functions.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit

from ..records import canonical_object_id
from .location_gate import (
    _REVISION as _REV_SHAPE,
)
from .location_gate import (
    RepositoryConfigError,
    TwoHopKeyError,
    _is_canonical_path,
    parse_repository,
)
from .transport import GuardedTransport, RemoteIOError, TwoHopResult

PAGE_SIZE = 200
MAX_PAGES = 50  # provider request ceiling; result is capped earlier
MAX_LISTED = 1000  # bounded inspect memory; never full-repo crawl
_SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class ListedFile:
    path: str
    size: int
    provider_sha256: str | None
    lfs: bool
    revision_candidate: str = ""


@dataclass(frozen=True)
class TwoHopProbe:
    """Sanitized two-hop result bound to the tree object identity it probed.

    The probe identity (repository, revision candidate, path, size) comes from
    the tree listing entry, NOT from the caller: a later proof record may only
    use this bound size/path/revision, never an arbitrary caller value.
    Never carries a URL, signature, or credential.
    """

    repository: str
    revision_candidate: str
    path: str
    size: int
    result: TwoHopResult

    def as_dict(self) -> dict:
        return {"repository": self.repository,
                "revision_candidate": self.revision_candidate,
                "path": self.path, "size": self.size,
                **self.result.as_dict()}


class ModelScopeDataset:
    """One configured repo and one guarded metadata/download transport.

    repo_id is an explicit RUNTIME configuration string (owner/name), parsed
    exactly once by parse_repository and normalized on this instance. Every
    URL, tree listing, probe identity and proof record is derived from THIS
    instance's bound identity (endpoint + repository + fixed type profile);
    probes are never reusable against a different instance configuration.
    """

    def __init__(self, transport: GuardedTransport, endpoint: str,
                 repo_id: str):
        try:
            self.repository_id = parse_repository(repo_id)
        except RepositoryConfigError:
            raise ValueError(
                "repository configuration is not a valid owner/name") from None
        try:
            parsed = urlsplit(endpoint)
        except ValueError:
            parsed = None
        if parsed is None:
            raise ValueError("invalid provider endpoint")
        if (parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or parsed.username or parsed.password):
            raise ValueError("provider endpoint must be origin only")
        transport._host(endpoint)
        self.endpoint = endpoint.rstrip("/")
        self.transport = transport
        self.repo_id = self.repository_id.id
        self.base = (f"{self.endpoint}/api/v1/datasets"
                     f"/{self.repository_id.owner}/{self.repository_id.name}")

    def _data(self, url: str, *, phase: str = "provider_revision_shape") -> object:
        payload = self.transport.read_metadata(url)
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, ValueError):
            decoded = None
        if decoded is None:
            raise RemoteIOError("invalid provider metadata JSON", code="invalid_json",
                                phase=phase)
        if not isinstance(decoded, dict) or "Data" not in decoded:
            raise RemoteIOError("unrecognized provider response; cannot claim completeness",
                                code="provider_shape", phase=phase)
        return decoded["Data"]

    def revisions(self) -> list[str]:
        """List commit-id-shaped candidates; syntax does NOT verify immutability."""
        info = self._data(self.base + "/revisions")
        if not isinstance(info, dict) or not isinstance(info.get("RevisionMap"), dict):
            raise RemoteIOError("unrecognized revision map", code="provider_shape",
                                phase="provider_revision_shape")
        revisions = info["RevisionMap"]
        tags, branches = revisions.get("Tags"), revisions.get("Branches")
        if not isinstance(tags, list) or not isinstance(branches, list):
            raise RemoteIOError("unrecognized revision entries", code="provider_shape",
                                phase="provider_revision_shape")
        candidates = tags + branches
        result = set()
        for item in candidates:
            if not isinstance(item, dict):
                raise RemoteIOError("malformed revision entry", code="provider_shape",
                                    phase="provider_revision_shape")
            for field in ("CommitId", "CommitID", "commit_id", "Revision"):
                value = item.get(field)
                if isinstance(value, str) and _SHA.fullmatch(value):
                    result.add(value)
        return sorted(result)

    def list_files(self, revision: str) -> tuple[list[ListedFile], bool]:
        """Bounded repo/tree pagination; completeness needs consistent Total."""
        if not isinstance(revision, str) or not _SHA.fullmatch(revision):
            raise ValueError("directory listing requires a commit-ID-shaped revision")
        found: dict[str, ListedFile] = {}
        seen_paths: set[str] = set()  # trees and blobs across every page
        seen_entries = 0
        declared_total: int | None = None
        total_profile: bool | None = None
        for page in range(1, MAX_PAGES + 1):
            params = urlencode({"Revision": revision, "Recursive": "True",
                                "PageNumber": page, "PageSize": PAGE_SIZE})
            data = self._data(self.base + "/repo/tree?" + params,
                              phase="provider_listing_shape")
            # Design r5 section 5.5: the legacy tree payload reports the entry total
            # under the field name TotalCount; the provider contract name is Total.
            # Same wire value, single mapping rule; never mix the two names.
            if isinstance(data, dict) and "Total" not in data and "TotalCount" in data:
                data = {**data, "Total": data["TotalCount"]}
            if (isinstance(data, dict) and "Total" in data
                    and "TotalCount" in data
                    and data["Total"] != data["TotalCount"]):
                raise RemoteIOError("provider listing total fields disagree")
            entries = (data.get("Files", data.get("files"))
                       if isinstance(data, dict) else data)
            has_total = isinstance(data, dict) and "Total" in data
            if total_profile is not None and has_total != total_profile:
                raise RemoteIOError("inconsistent provider listing total presence")
            total_profile = has_total
            if has_total:
                total = data["Total"]
                if type(total) is not int or total < 0 or (
                        declared_total is not None and declared_total != total):
                    raise RemoteIOError("inconsistent provider listing total")
                declared_total = total
            if not isinstance(entries, list) or len(entries) > PAGE_SIZE:
                raise RemoteIOError("invalid or truncated provider listing")
            seen_entries += len(entries)  # Total counts all tree and blob entries
            for item in entries:
                if not isinstance(item, dict):
                    raise RemoteIOError("invalid file-tree entry")
                path = item.get("Path", item.get("path"))
                try:
                    canonical_object_id(path)
                except (TypeError, ValueError):
                    path = None
                if path is None or len(path.encode("utf-8")) > 512:
                    raise RemoteIOError("unsafe or excessive provider file path")
                if path in seen_paths:
                    raise RemoteIOError("provider returned duplicate tree/blob path")
                seen_paths.add(path)
                kind = item.get("Type", item.get("type", "blob"))
                if kind == "tree":
                    continue
                if kind != "blob":
                    raise RemoteIOError("unsupported provider entry type")
                size = item.get("Size", item.get("size"))
                if type(size) is not int or not 0 <= size < 2**64:
                    raise RemoteIOError("provider file size absent or invalid")
                digest = item.get("Sha256", item.get("sha256"))
                if digest is not None and (not isinstance(digest, str)
                                           or not re.fullmatch(r"[0-9a-f]{64}", digest)):
                    raise RemoteIOError("provider file SHA256 is malformed")
                record = ListedFile(path, size, digest, bool(item.get("Lfs", False)),
                                    revision_candidate=revision)
                found[path] = record
            if declared_total is not None:
                if seen_entries > declared_total:
                    raise RemoteIOError("provider returned more entries than declared")
                if seen_entries == declared_total:
                    return sorted(found.values(), key=lambda x: x.path), True
                if not entries:
                    raise RemoteIOError("provider listing ended before declared total")
            if len(found) >= MAX_LISTED:
                return sorted(found.values(), key=lambda x: x.path), False
            if declared_total is None and len(entries) < PAGE_SIZE:
                # A short page alone has not been verified as the complete
                # provider listing; never assert full-repo coverage from it.
                return sorted(found.values(), key=lambda x: x.path), False
        # Hard cap: no assertion that a partial listing covers this repo.
        return sorted(found.values(), key=lambda x: x.path), False

    def download_url(self, revision: str, path: str) -> str:
        """Official legacy get_download_url pattern; ephemeral only, never save."""
        if not isinstance(revision, str) or not _SHA.fullmatch(revision):
            raise ValueError("cannot download from floating revision")
        try:
            canonical_object_id(path)
        except (TypeError, ValueError):
            path = None
        if path is None:
            raise ValueError("unsafe provider file path")
        params = urlencode({"Revision": revision, "FilePath": path})
        return f"{self.base}/repo?{params}"

    def range_probe(self, entry: "ListedFile", *, start: int, length: int,
                    batch: str = "") -> "TwoHopProbe":
        """Guarded two-hop capability read of ONE tree-listed object range.

        The object identity (revision candidate, path) and expected size all
        come from the tree listing entry — the caller cannot supply an
        arbitrary size/path/revision. hop1 is the fixed owner/name /repo URL
        (Bearer + same-origin session cookie); a 302 Location is validated in
        memory only and, when it passes the strict gate, hop2 is a fresh
        credential-free request to the exact host observed in THAT hop-1
        (one-shot, in memory, never cached across requests/objects). Returns
        a sanitized TwoHopProbe bound to the tree identity; the raw
        Location/signature never leaves the process.
        """
        if not isinstance(entry, ListedFile):
            raise ValueError("range_probe requires a tree-listed entry")
        if not _is_canonical_path(entry.path):
            raise ValueError("tree entry path not canonical")
        if not _REV_SHAPE.fullmatch(entry.revision_candidate):
            raise ValueError("tree entry revision candidate is not commit-shaped")
        if type(entry.size) is not int or not 0 < entry.size < 2**64:
            raise ValueError("tree entry size must be a positive bounded integer")
        url = self.download_url(entry.revision_candidate, entry.path)
        result = self.transport.two_hop_range(
            url, start=start, length=length, expected_size=entry.size,
            batch=batch)
        return TwoHopProbe(repository=self.repo_id,
                           revision_candidate=entry.revision_candidate,
                           path=entry.path, size=entry.size, result=result)

    def record_probe_proof(self, probe: "TwoHopProbe", payload_sha: str,
                           observed_batch: str = "") -> str:
        """Store a v2 proof for an actual probe, using ONLY the bound identity.

        size/path/revision come from the probe's tree binding; the caller
        cannot substitute arbitrary values. No network is made.
        """
        if not isinstance(probe, TwoHopProbe):
            raise ValueError("record_probe_proof requires a TwoHopProbe")
        # Cross-instance/cross-repository reuse is refused: the probe identity
        # must belong to THIS instance's normalized configuration, and the
        # type profile stays fixed (never relaxed to another descriptor).
        if probe.repository != self.repo_id:
            raise TwoHopKeyError("probe does not belong to this repository")
        repository_type = "modelscope_dataset_legacy"
        return self.transport.record_two_hop_proof(
            origin_endpoint=self.endpoint,
            repository=probe.repository,
            repository_type=repository_type,
            revision=probe.revision_candidate,
            path=probe.path,
            size=probe.size,
            etag=probe.result.etag,
            cdn_host=probe.result.approved_host,
            payload_sha=payload_sha,
            observed_batch=observed_batch or probe.result.batch)
