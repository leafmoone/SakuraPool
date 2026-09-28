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
from .transport import GuardedTransport, RemoteIOError

REPO_ID = "leafmoone/game_cg_5M"
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


class ModelScopeDataset:
    """One fixed repo/type and one guarded metadata/download transport."""

    def __init__(self, transport: GuardedTransport, endpoint: str,
                 repo_id: str = REPO_ID):
        if repo_id != REPO_ID:
            raise ValueError("only the authorized dataset repo may be inspected")
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
        self.repo_id = repo_id
        self.base = f"{self.endpoint}/api/v1/datasets/{REPO_ID}"

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
                record = ListedFile(path, size, digest, bool(item.get("Lfs", False)))
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
