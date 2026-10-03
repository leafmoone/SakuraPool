"""Task-specific connection profile; legacy single-object scope is unchanged."""

import json
import os
from pathlib import Path

from ..fs_safety import plain_entry
from ..storage.location_gate import parse_repository
from ..storage.production import RustProductionTransport
from .store import TaskError

FORMAT = "sakurapool-task-connection-v1"


def read_profile(path):
    path = plain_entry(path)
    if path.stat().st_size > 65536:
        raise TaskError("PROFILE_TOO_LARGE", "profile")
    profile = json.loads(path.read_bytes())
    if (not isinstance(profile, dict) or set(profile) - {
            "format", "origin", "repositories", "worker", "credential_ref"}
            or profile.get("format") != FORMAT
            or profile.get("origin") not in ("https://modelscope.cn", "https://www.modelscope.cn")
            or not isinstance(profile.get("worker"), str)):
        raise TaskError("PROFILE_INVALID", "profile")
    repos = profile.get("repositories")
    if not isinstance(repos, list) or not 1 <= len(repos) <= 128:
        raise TaskError("PROFILE_ALLOWLIST_INVALID", "profile")
    for repo in repos:
        parse_repository(repo)
    ref = profile.get("credential_ref")
    if ref is not None and (not isinstance(ref, dict) or len(ref) != 1
                            or not set(ref).issubset({"env", "file"})):
        raise TaskError("PROFILE_CREDENTIAL_REF_INVALID", "profile")
    plain_entry(Path(profile["worker"]))
    return profile


def validate_allowlist(profile, publication):
    for endpoint, repo in publication.catalog.execute("SELECT endpoint,repo_id FROM repositories"):
        if endpoint != profile["origin"] or repo not in profile["repositories"]:
            raise TaskError("PROFILE_SCOPE_MISMATCH", "profile")


def connect_profile(profile, ledger):
    ref = profile.get("credential_ref")
    token = None
    if ref is not None:
        if "env" in ref:
            if ref["env"] != "MODELSCOPE_API_TOKEN":
                raise TaskError("PROFILE_CREDENTIAL_REF_INVALID", "profile")
            token = os.environ.get(ref["env"]) or None
        else:
            path = plain_entry(ref["file"])
            if path.stat().st_size > 4096:
                raise TaskError("PROFILE_CREDENTIAL_INVALID", "profile")
            token = path.read_text(encoding="utf-8").strip()
    if token and (len(token) > 4096 or any(ord(c) < 0x21 or ord(c) >= 0x7f for c in token)):
        raise TaskError("PROFILE_CREDENTIAL_INVALID", "profile")
    return RustProductionTransport(ledger, Path(profile["worker"]), origin=profile["origin"],
                                   token=token, same_origin_cookie=("m_session_id=" + token)
                                   if token else None)
