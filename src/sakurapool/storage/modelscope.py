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


class TreePage(tuple):
    """Legacy two-tuple compatibility with unfiltered pagination evidence.

    `complete` is page-local scoped completeness; a walk must additionally
    account for all raw entries and stable totals across its requested scope.
    """

    def __new__(cls, files, complete, *, raw_count, total, raw_paths, continuation):
        result = super().__new__(cls, (files, complete))
        result.raw_count = raw_count
        result.total = total
        result.raw_paths = tuple(raw_paths)
        result.continuation = continuation
        return result

    @property
    def files(self):
        return self[0]

    @property
    def complete(self):
        return self[1]


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


def _io_error(provider, message, **kwargs):
    # Only the reader's explicit evidence is composed; plain fixtures are unknown.
    kwargs["accounting"] = ("CONFIRMED" if getattr(provider, "_metadata_reads", 0)
                            and provider._metadata_confirmed else "UNKNOWN")
    return RemoteIOError(message, **kwargs)


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
        self._metadata_reads = 0
        self._metadata_confirmed = True
        self.repo_id = self.repository_id.id
        self.base = (f"{self.endpoint}/api/v1/datasets"
                     f"/{self.repository_id.owner}/{self.repository_id.name}")

    def _data(
        self, url: str, *, phase: str = "provider_revision_shape", tree_retry: bool = False
    ) -> object:
        request_phase = {
            "provider_repository_shape": "provider_repository_request",
            "provider_tree_shape": "provider_tree_request",
        }.get(phase)
        try:
            if tree_retry:
                payload = self.transport.read_metadata(url, _validated_tree_retry=True)
            else:
                payload = self.transport.read_metadata(url)
        except RemoteIOError as error:
            self._metadata_reads += 1
            self._metadata_confirmed &= error.accounting_state == "CONFIRMED"
            error.accounting_state = "CONFIRMED" if self._metadata_confirmed else "UNKNOWN"
            if request_phase is not None:
                error.phase = request_phase
            raise
        except Exception:
            self._metadata_confirmed = False
            raise RemoteIOError(
                "metadata operation failed",
                phase=request_phase or "metadata_send",
                accounting="UNKNOWN",
            ) from None
        self._metadata_reads += 1
        self._metadata_confirmed &= getattr(payload, "accounting_state", "UNKNOWN") == "CONFIRMED"
        if not self._metadata_confirmed:
            raise RemoteIOError(
                "metadata operation contains an uncertain request",
                code="network_ambiguous",
                phase=request_phase or "metadata_body",
                accounting="UNKNOWN",
            )
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, ValueError):
            decoded = None
        if decoded is None:
            raise _io_error(
                self, "invalid provider metadata JSON", code="invalid_json", phase=phase
            )
        if not isinstance(decoded, dict) or "Data" not in decoded:
            raise _io_error(
                self,
                "unrecognized provider response; cannot claim completeness",
                code="provider_shape",
                phase=phase,
            )
        if decoded.get("Code", 200) != 200 or decoded.get("Success") is False:
            raise _io_error(
                self, "provider metadata unsuccessful", code="provider_rejection", phase=phase
            )
        return decoded["Data"]

    def legacy_hub_id(self) -> int:
        """Resolve SDK legacy numeric ID only after exact owner/name validation."""
        info = self._data(self.base, phase="provider_repository_shape")
        if (not isinstance(info, dict)
                or info.get("Namespace") != self.repository_id.owner
                or info.get("Name") != self.repository_id.name
                or type(info.get("Id")) is not int or not 0 < info["Id"] < 1 << 63
                or type(info.get("Type")) is not int or info["Type"] != 4):
            raise _io_error(self, "provider legacy repository identity differs",
                            code="provider_shape",
                              phase="provider_repository_shape")
        self._legacy_verified_id = info["Id"]
        return info["Id"]

    def legacy_tree_page(
        self, hub_id: int, revision: str, *, root: str, page: int = 1, page_size: int = 20
    ) -> TreePage:
        """Bounded SDK legacy tree; master only discovers candidates, never proves binding."""
        if (
            type(hub_id) is not int
            or hub_id != getattr(self, "_legacy_verified_id", None)
            or not 0 < hub_id < 1 << 63
            or not isinstance(revision, str)
            or (revision != "master" and not _SHA.fullmatch(revision))
            or type(page) is not int
            or not 1 <= page <= MAX_PAGES
            or type(page_size) is not int
            or not 1 <= page_size <= PAGE_SIZE
            or not isinstance(root, str)
            or (root != "/" and not _is_canonical_path(root))
        ):
            raise ValueError("legacy tree scope invalid")
        query = urlencode(
            {
                "Revision": revision,
                "Root": root,
                "Recursive": "True",
                "PageNumber": page,
                "PageSize": page_size,
            }
        )
        info = self._data(
            f"{self.endpoint}/api/v1/datasets/{hub_id}/repo/tree?{query}",
            phase="provider_tree_shape",
            tree_retry=bool(_SHA.fullmatch(revision)) and self._metadata_confirmed,
        )
        files = info.get("Files") if isinstance(info, dict) else None
        if not isinstance(files, list) or len(files) > page_size:
            raise _io_error(
                self,
                "unrecognized legacy tree page",
                code="provider_page_shape",
                phase="provider_listing_shape",
            )
        result = []
        seen = set()
        for entry in files:
            if not isinstance(entry, dict):
                raise _io_error(
                    self,
                    "malformed legacy tree entry",
                    code="provider_entry_shape",
                    phase="provider_listing_shape",
                )
            if entry.get("Type") not in ("blob", "file"):
                if entry.get("Type") in ("tree", "directory"):
                    directory = entry.get("Path")
                    if (
                        not isinstance(directory, str)
                        or not _is_canonical_path(directory)
                        or len(directory) > 512
                        or (root != "/" and not directory.startswith(root + "/"))
                    ):
                        raise _io_error(
                            self,
                            "legacy tree directory scope invalid",
                            code="provider_entry_path",
                            phase="provider_listing_shape",
                        )
                    if directory in seen:
                        raise _io_error(
                            self,
                            "legacy tree path duplicate",
                            code="provider_entry_duplicate",
                            phase="provider_listing_shape",
                        )
                    seen.add(directory)
                    continue
                raise _io_error(
                    self,
                    "unrecognized legacy tree entry type",
                    code="provider_entry_type",
                    phase="provider_listing_shape",
                )
            path, size, candidate = entry.get("Path"), entry.get("Size"), entry.get("Revision")
            if not isinstance(path, str) or not _is_canonical_path(path) or len(path) > 512:
                raise _io_error(
                    self,
                    "legacy tree path invalid",
                    code="provider_entry_path",
                    phase="provider_listing_shape",
                )
            if path in seen:
                raise _io_error(
                    self,
                    "legacy tree path duplicate",
                    code="provider_entry_duplicate",
                    phase="provider_listing_shape",
                )
            if root != "/" and not path.startswith(root.rstrip("/") + "/"):
                raise _io_error(
                    self,
                    "legacy tree scope invalid",
                    code="provider_entry_scope",
                    phase="provider_listing_shape",
                )
            if type(size) is not int or not 0 <= size <= 1 << 50:
                raise _io_error(
                    self,
                    "legacy tree size invalid",
                    code="provider_entry_size",
                    phase="provider_listing_shape",
                )
            candidate_absent = candidate is None or candidate == ""
            if (revision == "master" or not candidate_absent) and (
                not isinstance(candidate, str) or not _SHA.fullmatch(candidate)
            ):
                raise _io_error(
                    self,
                    "legacy tree revision invalid",
                    code="provider_entry_revision_shape",
                    phase="provider_listing_shape",
                )
            # Entry Revision is optional last-modified metadata for a pinned
            # snapshot. Only master discovery needs it to identify a candidate;
            # pinned tree identities and download URLs always use the request.
            effective_revision = candidate if revision == "master" else revision
            seen.add(path)
            sha = entry.get("Sha256")
            if sha is not None and (
                not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None
            ):
                raise _io_error(
                    self,
                    "legacy tree digest malformed",
                    code="provider_entry_digest",
                    phase="provider_listing_shape",
                )
            result.append(ListedFile(path, size, sha, False, effective_revision))
        total = info.get("TotalCount", info.get("Total"))
        if "Total" in info and "TotalCount" in info and info["Total"] != info["TotalCount"]:
            raise _io_error(
                self,
                "provider listing total fields disagree",
                code="provider_total_conflict",
                phase="provider_listing_shape",
            )
        if total is not None and (
            type(total) is not int or total < len(files) or (not files and total > 0)
        ):
            raise _io_error(
                self,
                "provider listing total contradicts raw entries",
                code="provider_total_conflict",
                phase="provider_listing_shape",
            )
        complete = (page == 1 and total == len(files)) or (total is None and not files)
        return TreePage(
            result,
            complete,
            raw_count=len(files),
            total=total,
            raw_paths=seen,
            continuation=bool(files) and not complete,
        )

    def find_legacy_file(
        self, hub_id, revision, *, root, path, page_size=PAGE_SIZE, max_pages=MAX_PAGES
    ):
        """One bounded exact lookup; filtered-empty pages never establish EOF."""
        for page in ModelScopeDataset.iter_legacy_pages(
            self, hub_id, revision, root=root, page_size=page_size, max_pages=max_pages
        ):
            matches = [row for row in page.files if row.path == path]
            if len(matches) > 1:
                raise _io_error(self, "provider exact object duplicate")
            if matches:
                return matches[0]
        raise _io_error(
            self,
            "provider exact object unavailable",
            code="provider_object_absent",
            phase="provider_exact_lookup",
        )

    def iter_legacy_pages(
        self, hub_id, revision, *, root, page_size=PAGE_SIZE, max_pages=MAX_PAGES
    ):
        """Shared scope walk, yielding bounded pages with consistent raw evidence."""
        if type(max_pages) is not int or not 1 <= max_pages <= MAX_PAGES:
            raise ValueError("tree page bound invalid")
        raw_count = 0
        paths = set()
        total_profile = None
        for number in range(1, max_pages + 1):
            page = self.legacy_tree_page(
                hub_id, revision, root=root, page=number, page_size=page_size
            )
            if not isinstance(page, TreePage):
                # Compatibility for old complete tuple providers; an incomplete
                # filtered page has insufficient evidence, never means absent.
                rows, complete = page
                if not complete:
                    raise _io_error(
                        self,
                        "provider listing incomplete: raw evidence unavailable",
                        code="provider_listing_incomplete",
                        phase="provider_exact_lookup",
                    )
                page = TreePage(
                    rows,
                    True,
                    raw_count=len(rows),
                    total=len(rows),
                    raw_paths=[row.path for row in rows],
                    continuation=False,
                )
            profile = (page.total is not None, page.total)
            if total_profile is not None and profile != total_profile:
                raise _io_error(
                    self,
                    "provider listing total changed",
                    code="provider_total_conflict",
                    phase="provider_listing_shape",
                )
            total_profile = profile
            if paths.intersection(page.raw_paths):
                raise _io_error(
                    self,
                    "provider listing repeated page path",
                    code="provider_page_repeat",
                    phase="provider_listing_shape",
                )
            paths.update(page.raw_paths)
            raw_count += page.raw_count
            if page.total is not None and raw_count > page.total:
                raise _io_error(
                    self,
                    "provider listing raw count exceeds total",
                    code="provider_total_conflict",
                    phase="provider_listing_shape",
                )
            yield page
            if page.complete or (page.total is not None and raw_count == page.total):
                return
            if not page.continuation:
                return
        raise _io_error(
            self,
            "provider exact listing incomplete: page bound reached",
            code="provider_listing_incomplete",
            phase="provider_exact_lookup",
        )

    def revisions(self) -> list[str]:
        """List commit-id-shaped candidates; syntax does NOT verify immutability."""
        info = self._data(self.base + "/revisions")
        if not isinstance(info, dict) or not isinstance(info.get("RevisionMap"), dict):
            raise _io_error(
                self,
                "unrecognized revision map",
                code="provider_shape",
                phase="provider_revision_shape",
            )
        revisions = info["RevisionMap"]
        tags, branches = revisions.get("Tags"), revisions.get("Branches")
        if not isinstance(tags, list) or not isinstance(branches, list):
            raise _io_error(
                self,
                "unrecognized revision entries",
                code="provider_shape",
                phase="provider_revision_shape",
            )
        candidates = tags + branches
        result = set()
        for item in candidates:
            if not isinstance(item, dict):
                raise _io_error(
                    self,
                    "malformed revision entry",
                    code="provider_shape",
                    phase="provider_revision_shape",
                )
            for field in ("CommitId", "CommitID", "commit_id", "Revision"):
                value = item.get(field)
                if isinstance(value, str) and _SHA.fullmatch(value):
                    result.add(value)
        return sorted(result)

    def list_files(self, revision: str, *, max_pages: int = MAX_PAGES
                   ) -> tuple[list[ListedFile], bool]:
        """Bounded repo/tree pagination; completeness needs consistent Total."""
        if not isinstance(revision, str) or not _SHA.fullmatch(revision):
            raise ValueError("directory listing requires a commit-ID-shaped revision")
        if type(max_pages) is not int or not 1 <= max_pages <= MAX_PAGES:
            raise ValueError("tree page bound invalid")
        found: dict[str, ListedFile] = {}
        seen_paths: set[str] = set()  # trees and blobs across every page
        seen_entries = 0
        declared_total: int | None = None
        total_profile: bool | None = None
        for page in range(1, max_pages + 1):
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
                raise _io_error(self, "provider listing total fields disagree",
                                    code="provider_total_conflict", phase="provider_listing_shape")
            entries = (data.get("Files", data.get("files"))
                       if isinstance(data, dict) else data)
            has_total = isinstance(data, dict) and "Total" in data
            if total_profile is not None and has_total != total_profile:
                raise _io_error(self, "inconsistent provider listing total presence")
            total_profile = has_total
            if has_total:
                total = data["Total"]
                if type(total) is not int or total < 0 or (
                        declared_total is not None and declared_total != total):
                    raise _io_error(self, "inconsistent provider listing total")
                declared_total = total
            if not isinstance(entries, list) or len(entries) > PAGE_SIZE:
                raise _io_error(self, "invalid or truncated provider listing")
            seen_entries += len(entries)  # Total counts all tree and blob entries
            for item in entries:
                if not isinstance(item, dict):
                    raise _io_error(self, "invalid file-tree entry")
                path = item.get("Path", item.get("path"))
                try:
                    canonical_object_id(path)
                except (TypeError, ValueError):
                    path = None
                if path is None or len(path.encode("utf-8")) > 512:
                    raise _io_error(self, "unsafe or excessive provider file path")
                if path in seen_paths:
                    raise _io_error(self, "provider returned duplicate tree/blob path")
                seen_paths.add(path)
                kind = item.get("Type", item.get("type", "blob"))
                if kind == "tree":
                    continue
                if kind != "blob":
                    raise _io_error(self, "unsupported provider entry type")
                size = item.get("Size", item.get("size"))
                if type(size) is not int or not 0 <= size < 2**64:
                    raise _io_error(self, "provider file size absent or invalid")
                digest = item.get("Sha256", item.get("sha256"))
                if digest is not None and (not isinstance(digest, str)
                                           or not re.fullmatch(r"[0-9a-f]{64}", digest)):
                    raise _io_error(self, "provider file SHA256 is malformed")
                record = ListedFile(path, size, digest, bool(item.get("Lfs", False)),
                                    revision_candidate=revision)
                found[path] = record
            if declared_total is not None:
                if seen_entries > declared_total:
                    raise _io_error(self, "provider returned more entries than declared")
                if seen_entries == declared_total:
                    return sorted(found.values(), key=lambda x: x.path), True
                if not entries:
                    raise _io_error(self, "provider listing ended before declared total")
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

    def range_probe(
        self, entry: "ListedFile", *, start: int, length: int, batch: str = ""
    ) -> "TwoHopProbe":
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
            url, start=start, length=length, expected_size=entry.size, batch=batch
        )
        return TwoHopProbe(
            repository=self.repo_id,
            revision_candidate=entry.revision_candidate,
            path=entry.path,
            size=entry.size,
            result=result,
        )

    def record_probe_proof(
        self, probe: "TwoHopProbe", payload_sha: str, observed_batch: str = ""
    ) -> str:
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
            observed_batch=observed_batch or probe.result.batch,
        )
