"""Local workspace administration and credential-free diagnostics."""

import json

from .capacity import CapacityConfig, ResourcePolicy
from .workspace import Workspace


def add_parser(subparsers):
    workspace = subparsers.add_parser("workspace")
    commands = workspace.add_subparsers(dest="workspace_command", required=True)
    for name in ("init", "inspect", "update"):
        command = commands.add_parser(name)
        command.add_argument("root")
        if name in ("init", "update"):
            command.add_argument("--config", required=name == "update")
        if name == "update":
            command.add_argument(
                "--expected-version",
                "--expected-epoch",
                type=int,
                help="compare-and-swap policy epoch; omitted means last-writer-wins replacement",
            )
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
            if set(config) - {"capacity", "policy"}:
                raise ValueError("unknown workspace configuration")
            workspace = Workspace.init(
                args.root,
                capacity=CapacityConfig.from_configuration(config.get("capacity")),
                policy=ResourcePolicy.from_configuration(config.get("policy")),
            )
            result = workspace.inspect()
        else:
            workspace = Workspace.open(args.root)
            if args.workspace_command == "update":
                config = _config(args.config)
                if set(config) == {"policy"}:
                    config = config["policy"]
                if not isinstance(config, dict) or set(config) - set(
                    ResourcePolicy.__dataclass_fields__
                ):
                    raise ValueError("only resource policy can be updated")
                policy = ResourcePolicy.from_dict({**workspace.policy.to_dict(), **config})
                epoch = workspace.update_policy(policy, expected_version=args.expected_version)
                semantics = (
                    "EXPLICIT_EPOCH_CAS"
                    if args.expected_version is not None
                    else "LAST_WRITER_WINS"
                )
                update = {"update_semantics": semantics, "updated_policy_version": epoch}
            result = workspace.inspect()
            if args.workspace_command == "update":
                result.update(update)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"code": "WORKSPACE_FAILED", "phase": "local", "network": "NOT_STARTED"}))
        return 2
