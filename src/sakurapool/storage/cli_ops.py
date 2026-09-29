"""Small explicit P4 CLI boundary; offline fixtures never reach public hosts.

The operator gate for *running* on ModelScope is external to this module.
Production scan/compile remains blocked until the 4 GiB working-set budget
is proven.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import Path
from urllib.parse import urlsplit

from .budget import DEFAULT_WORK_ROOT, BudgetExceeded, BudgetLedger, Reservation, _cluster_bytes
from .location_gate import RepositoryConfigError, parse_repository
from .modelscope import ModelScopeDataset
from .package import fetch_from_package, load_package
from .transport import GuardedTransport

_MAX_CONFIG = 16 << 10
_MAX_PLAN = 1 << 20
_REVISION = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def _profile(path: Path, *, offline_fixture: bool,
             require_revision: bool = False) -> tuple[dict, BudgetLedger]:
    if not path.is_file() or path.stat().st_size > _MAX_CONFIG:
        raise ValueError("storage profile missing or too large")
    try:
        data = json.loads(path.read_bytes())
    except (UnicodeError, ValueError):
        raise ValueError("invalid storage profile JSON") from None
    if (not isinstance(data, dict) or set(data) != {
            "repo_id", "endpoint", "revision", "trusted_hosts", "work_root"}):
        raise ValueError("unknown or incomplete storage profile")
    try:
        parse_repository(data["repo_id"])
    except (RepositoryConfigError, TypeError):
        raise ValueError("profile repository is not a valid owner/name") from None
    endpoint, revision = data["endpoint"], data["revision"]
    if (not isinstance(endpoint, str) or (revision is None and require_revision)
            or (revision is not None and (not isinstance(revision, str)
                                           or not _REVISION.fullmatch(revision)))):
        raise ValueError("profile requires an explicit commit ID candidate or null bootstrap")
    try:
        parts = urlsplit(endpoint)
        port = parts.port
    except ValueError:
        raise ValueError("invalid storage endpoint") from None
    if (parts.path not in ("", "/") or parts.query or parts.fragment
            or parts.username or parts.password):
        raise ValueError("storage endpoint must be an origin, without secrets")
    hosts = data["trusted_hosts"]
    if (not isinstance(hosts, list) or not hosts or len(hosts) > 8
            or any(not isinstance(host, str) for host in hosts)
            or len(set(hosts)) != len(hosts)):
        raise ValueError("invalid trusted host profile")
    if offline_fixture:
        if parts.scheme != "http" or parts.hostname != "127.0.0.1" \
                or port is None or hosts != ["127.0.0.1"]:
            raise ValueError("offline fixture requires literal 127.0.0.1 only")
    elif (endpoint.rstrip("/") != "https://modelscope.cn" or port is not None
          or hosts != ["modelscope.cn"]):
        # CDN hosts require audited expansion; never send profile credentials
        # to a dynamically supplied trusted host.
        raise ValueError("production profile requires the fixed audited origin")
    root = Path(data["work_root"]).absolute()
    if (not offline_fixture and root != DEFAULT_WORK_ROOT.absolute()
            or offline_fixture and (root == DEFAULT_WORK_ROOT.absolute()
                                    or not root.is_relative_to(DEFAULT_WORK_ROOT.absolute()))):
        raise ValueError("incorrect production/offline budget root")
    return data, BudgetLedger(root, _offline_test=offline_fixture)


def _transport(ledger: BudgetLedger, config: dict) -> GuardedTransport:
    if ledger.offline_mode:
        return GuardedTransport(ledger, trusted_hosts=frozenset({"127.0.0.1"}),
                                allow_loopback_http=True)
    token = os.environ.get("MODELSCOPE_API_TOKEN") or None
    return GuardedTransport(ledger, trusted_hosts=frozenset({"modelscope.cn"}),
                            token=token, credential_origin=(config["endpoint"] if token else None))


def _fresh_output(output: Path, ledger: BudgetLedger) -> Path:
    output = output.absolute()
    parent = output.parent
    if (not parent.is_relative_to(ledger.root) or not parent.is_dir()
            or output.exists() or output.is_symlink()):
        raise ValueError("plan destination must be new under the fixed work root")
    if (parent.is_symlink() or (hasattr(parent, "is_junction") and parent.is_junction())
            or not parent.resolve().is_relative_to(ledger.root.resolve())):
        raise ValueError("unsafe plan destination")
    return output


def _plan_disk_reservation(cluster: int) -> int:
    """Allocated upper bound for both plan names plus two bounded clusters."""
    if cluster < 512 or cluster > _MAX_PLAN or cluster & (cluster - 1):
        raise ValueError("unsupported plan allocation cluster")
    return 2 * ((_MAX_PLAN + cluster - 1) // cluster * cluster) + 2 * cluster


def inspect(config_path: Path, output: Path, *, offline_fixture: bool = False) -> dict:
    """Metadata-only discovery; never selects a scan or claims source/tags."""
    config, ledger = _profile(config_path, offline_fixture=offline_fixture)
    output = _fresh_output(output, ledger)
    # Charge allocation, not EOF: temporary + published name are conservatively
    # counted twice even though publication hardlinks the same file. Reserve
    # two more clusters for bounded CLI status/log and an empty file. Root
    # growth elsewhere still counts via the shared ledger's allocation scan.
    cluster = _cluster_bytes(ledger.root)
    lease = ledger.reserve(Reservation(disk=_plan_disk_reservation(cluster)))
    tmp: Path | None = None
    owned: tuple[int, int] | None = None
    tmp_created = False
    published = False
    settled = False
    try:
        with _transport(ledger, config) as transport:
            provider = ModelScopeDataset(transport, config["endpoint"],
                                         config["repo_id"])
            revisions = provider.revisions()
            if config["revision"] is not None and config["revision"] not in revisions:
                raise ValueError("selected commit absent from guarded revision response")
            # Bootstrap lists only revision metadata. Never infer an immutable
            # binding or automatically continue to repo/tree with a null pin.
            files, complete = (provider.list_files(config["revision"])
                               if config["revision"] is not None else ([], False))
            plan = {
                "format": "sakurapool-p4-inspect-v1",
                "repository": config["repo_id"], "repo_type": "dataset",
                "endpoint": config["endpoint"],
                "requested_revision": config["revision"],
                "revision_candidates": revisions[:64],
                "revision_candidates_truncated": len(revisions) > 64,
                # Provider metadata is not proof of a verified immutable pin.
                "resolved_revision": None,
                "version_capability_verified": False,
                "conditional_capability_verified": False,
                "files_complete": complete,
                "files_completeness_basis": ("provider_declared_total" if complete
                                             else "unknown_or_truncated"),
                "source_and_tag_mapping": "unconfirmed; scan forbidden until verified",
                "files": [{"path": f.path, "size": f.size,
                           "provider_sha256": f.provider_sha256, "lfs": f.lfs}
                          for f in files],
                "offline_fixture": ledger.offline_mode,
            }
            raw = json.dumps(plan, sort_keys=True, ensure_ascii=False).encode("utf-8")
            if len(raw) > _MAX_PLAN:
                raise ValueError("inspect plan exceeds bounded output; no file written")
            tmp = output.with_name(".sakurapool-inspect-" + secrets.token_hex(16))
            with tmp.open("xb") as stream:
                tmp_created = True
                st = os.fstat(stream.fileno())
                if not st.st_ino:
                    raise OSError("cannot establish owned inspect temporary file")
                owned = (st.st_dev, st.st_ino)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.link(tmp, output)  # create-new atomic publication, never replace
            published = True
            ledger.settle(lease)
            settled = True
            return {"status": "inspected", "revision": config["revision"],
                    "file_count": len(files), "files_complete": complete,
                    "source_and_tag_mapping": "unconfirmed", "output_sha256":
                    hashlib.sha256(raw).hexdigest(), "offline_fixture": ledger.offline_mode}
    finally:
        if owned is not None and tmp is not None and os.path.lexists(tmp):
            current = tmp.stat(follow_symlinks=False)
            if (stat.S_ISREG(current.st_mode)
                    and not getattr(current, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                    and (current.st_dev, current.st_ino) == owned):
                tmp.unlink()
        # Only the disk-only plan lease is known unused if neither its owned
        # temp nor the published plan exists. Unknown HTTP leases remain in the
        # ledger and are NEVER refunded by this local publication cleanup.
        if (not settled and not published and (not tmp_created or owned is not None)
                and not os.path.lexists(output)
                and (tmp is None or not tmp_created or not os.path.lexists(tmp))):
            ledger.settle(lease)


def fetch(package_root: Path, config_path: Path, record_id: str, output: Path,
          *, offline_fixture: bool = False) -> dict:
    """Never indexes: require an existing verified package and matching profile."""
    if not offline_fixture:
        raise BudgetExceeded("P4 production fetch BLOCKED: 4 GiB working-set budget unproven")
    config, ledger = _profile(config_path, offline_fixture=True,
                              require_revision=True)
    package = load_package(package_root, allow_offline_loopback=ledger.offline_mode)
    if (package.endpoint != config["endpoint"] or package.repo_id != config["repo_id"]
            or package.data_revision != config["revision"]):
        raise ValueError("profile differs from immutable package binding")
    with _transport(ledger, config) as transport:
        dest = fetch_from_package(package_root, record_id, output, transport)
    return {"status": "fetched", "record_id": record_id, "output": str(dest),
            "offline_fixture": ledger.offline_mode}
