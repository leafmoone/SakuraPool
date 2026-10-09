"""Task CLI: lightweight state; archived quota tasks never gain a writable connection."""

import argparse
import json
from pathlib import Path

from ..workspace import Workspace
from .context import LEGACY_CAPACITY
from .store import TaskDB, TaskError


def _positive_workers(value):
    try:
        workers = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("workers must be a positive integer") from None
    if workers < 1:
        raise argparse.ArgumentTypeError("workers must be a positive integer")
    return workers


def add_parser(subparsers):
    task = subparsers.add_parser("task")
    commands = task.add_subparsers(dest="task_command", required=True)
    for name in ("create", "start"):
        create = commands.add_parser(name)
        for option in ("publication", "query", "task-dir"):
            create.add_argument("--" + option, required=True)
        create.add_argument("--workspace")
        create.add_argument("--selection", choices=("all", "first", "sample", "records"),
                            default="all")
        create.add_argument("--limit", type=int)
        create.add_argument("--seed")
        create.add_argument("--records", help="bounded JSON record_id list")
        create.add_argument("--metadata", action="store_true")
        create.add_argument("--image-extensions", help="comma-separated registered suffixes")
        create.add_argument("--filename-template", default="{tag}_{index}")
        create.add_argument("--filename-prefix")
        if name == "start":
            create.add_argument("--profile", required=True)
            create.add_argument("--workers", type=_positive_workers, default=6)
            create.add_argument("--workers-per-tar", type=_positive_workers, default=6,
                                help="maximum active operations per TAR (default: 6)")
    update = commands.add_parser("update")
    update.add_argument("task_dir")
    update.add_argument("--workspace")
    update.add_argument("--expected-settings-version", type=int, required=True)
    update.add_argument("--image-extensions", required=True, help="safe raw-byte format superset")
    for name in ("inspect", "run", "pause", "cancel", "resume", "export"):
        command = commands.add_parser(name)
        command.add_argument("task_dir")
        command.add_argument("--workspace")
        if name == "inspect":
            command.add_argument("--failure-seq", type=int, help="read a bounded persisted failure")
        elif name in ("run", "resume"):
            command.add_argument("--profile", required=True)
            command.add_argument("--workers", type=_positive_workers, default=6)
            command.add_argument("--workers-per-tar", type=_positive_workers, default=6,
                                 help="maximum active operations per TAR (default: 6)")
        elif name == "export":
            command.add_argument("--manifest", required=True)


def bounded_json(path, cap=65536):
    with Path(path).open("rb") as stream:
        data = stream.read(cap + 1)
    if len(data) > cap:
        raise TaskError("INPUT_SIZE_LIMIT", "cli")
    return json.loads(data)


def command(args):
    try:
        action = args.task_command
        explicit = getattr(args, "workspace", None)
        if action in ("create", "start"):
            workspace = Workspace.open(explicit) if explicit is not None else None
            capacity = workspace.capacity if workspace is not None else LEGACY_CAPACITY
        else:
            with TaskDB(args.task_dir, readonly=True, workspace=explicit) as task:
                workspace, capacity = task.workspace, task.capacity
                task.validate_plan()
                if action == "inspect":
                    result = task.inspect()
                    if getattr(args, "failure_seq", None) is not None:
                        result["failure_diagnostic"] = task.failure_diagnostic(args.failure_seq)
                    print(json.dumps(result, sort_keys=True))
                    return 0
                if task.version != 4 and not (task.version == 3 and action == "export"):
                    raise TaskError("LEGACY_TASK_MIGRATION_REQUIRED", "plan")
        if action == "update":
            from .settings import update_task

            result = update_task(
                args.task_dir,
                expected_settings_version=args.expected_settings_version,
                extensions=args.image_extensions,
            )
        elif action in ("pause", "cancel"):
            with TaskDB(args.task_dir, workspace=workspace) as task:
                result = task.request("PAUSE" if action == "pause" else "CANCEL")
        elif action in ("create", "start"):
            from ..cli import _spec_from_dict
            from .plan import Selection
            from .runner import create_task

            records = (
                bounded_json(args.records, capacity.explicit_records_bytes) if args.records else []
            )
            if not isinstance(records, list):
                raise TaskError("RECORD_LIST_INVALID", "cli")
            selection = Selection(args.selection, args.limit, args.seed, tuple(records))
            query = _spec_from_dict(bounded_json(args.query, capacity.task_header_bytes))
            options = {"metadata": args.metadata, "image_extensions": args.image_extensions,
                       "filename_template": args.filename_template,
                       "filename_prefix": args.filename_prefix}
            if action == "start":
                from .profile import read_profile
                from .runner import create_and_run_task

                profile = read_profile(args.profile)
                result = create_and_run_task(
                    args.publication, args.task_dir, workspace, query, selection,
                    connection_profile=profile, workers=args.workers,
                    workers_per_tar=args.workers_per_tar, **options)
            else:
                with create_task(args.publication, args.task_dir, workspace, query,
                                 selection, **options) as task:
                    result = task.inspect()
        elif action == "export":
            from .export import export_task

            result = export_task(args.task_dir, args.manifest)
        else:
            from .profile import read_profile
            from .runner import run_task

            profile = read_profile(args.profile)
            result = run_task(
                args.task_dir,
                resume=action == "resume",
                connection_profile=profile,
                workers=args.workers,
                workers_per_tar=getattr(args, "workers_per_tar", 6),
            )
        print(json.dumps(result, sort_keys=True))
        return 0
    except TaskError as error:
        print(json.dumps(error.public_diagnostic(), sort_keys=True))
        return 2
    except Exception as error:
        diagnostics = {"exception_type": "untrusted_exception", "frames": []}
        try:
            from .diagnostic import safe_diagnostic

            diagnostics = safe_diagnostic(error)
        except BaseException:
            pass
        print(
            json.dumps(
                {
                    "code": "TASK_FAILED",
                    "phase": "task",
                    "recoverable": False,
                    "diagnostics": diagnostics,
                }
            )
        )
        return 2
