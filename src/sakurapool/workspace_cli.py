"""Local download workspace identity and credential-free diagnostics."""

import json

from .capacity import CapacityConfig
from .workspace import Workspace


def add_parser(subparsers):
    workspace = subparsers.add_parser("workspace")
    commands = workspace.add_subparsers(dest="workspace_command", required=True)
    for name in ("init", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("root")
        if name == "init":
            command.add_argument("--config")
    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("--workspace", required=True)
    doctor.add_argument("--profile")


def _config(path):
    if path is None:
        return {}
    from .tasks.cli import bounded_json

    value = bounded_json(path, 16384)
    if not isinstance(value, dict):
        raise ValueError("configuration must be an object")
    return value


def command(args):
    try:
        if args.command == "doctor":
            workspace = Workspace.open(args.workspace)
            result = {
                "status": "OK",
                "workspace": workspace.inspect(),
                "network": "NOT_STARTED",
                "credentials": "NOT_READ",
            }
            if args.profile is not None:
                from .tasks.profile import read_profile

                read_profile(args.profile)
                result["profile"] = "VALID"
        elif args.workspace_command == "init":
            config = _config(args.config)
            if set(config) - {"capacity"}:
                raise ValueError(
                    "download resource policy removed; only technical capacity allowed"
                )
            workspace = Workspace.init(
                args.root, capacity=CapacityConfig.from_configuration(config.get("capacity"))
            )
            result = workspace.inspect()
        else:
            result = Workspace.open(args.root).inspect()
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"code": "WORKSPACE_FAILED", "phase": "local", "network": "NOT_STARTED"}))
        return 2
