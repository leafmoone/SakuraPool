"""Thin task CLI routing."""

import json
from pathlib import Path

from ..storage.budget import DEFAULT_WORK_ROOT, BudgetLedger
from .store import TaskDB, TaskError


def add_parser(subparsers):
    task = subparsers.add_parser("task")
    commands = task.add_subparsers(dest="task_command", required=True)
    create = commands.add_parser("create")
    for option in ("publication", "query", "task-dir"):
        create.add_argument("--" + option, required=True)
    create.add_argument("--selection", choices=("all", "first", "sample", "records"), default="all")
    create.add_argument("--limit", type=int)
    create.add_argument("--seed")
    create.add_argument("--records", help="bounded JSON record_id list")
    create.add_argument("--metadata", action="store_true")
    create.add_argument("--max-output-bytes", type=int, default=512 << 20)
    for name in ("inspect", "run", "pause", "cancel", "resume", "export"):
        command = commands.add_parser(name)
        command.add_argument("task_dir")
        if name in ("run", "resume"):
            command.add_argument("--profile", required=True)
            command.add_argument("--workers", type=int, choices=(1, 2, 4), default=1)
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
        if action == "inspect":
            with TaskDB(args.task_dir, readonly=True) as task:
                result = task.inspect()
        elif action in ("pause", "cancel"):
            from .runner import admit_task_growth

            ledger = BudgetLedger(DEFAULT_WORK_ROOT)
            with admit_task_growth(args.task_dir, ledger), TaskDB(args.task_dir) as task:
                result = task.request("PAUSE" if action == "pause" else "CANCEL")
        else:
            ledger = BudgetLedger(DEFAULT_WORK_ROOT)
            if action == "create":
                from ..cli import _spec_from_dict
                from .plan import Selection
                from .runner import create_task

                records = bounded_json(args.records, 4 << 20) if args.records else []
                if not isinstance(records, list):
                    raise TaskError("RECORD_LIST_INVALID", "cli")
                selection = Selection(args.selection, args.limit, args.seed, tuple(records))
                with create_task(args.publication, args.task_dir, ledger,
                                 _spec_from_dict(bounded_json(args.query)), selection,
                                 metadata=args.metadata,
                                 max_output_bytes=args.max_output_bytes) as task:
                    result = task.inspect()
            elif action == "export":
                from .export import export_task

                result = export_task(args.task_dir, args.manifest, ledger)
            else:
                from .profile import connect_profile, read_profile
                from .runner import run_task

                profile = read_profile(args.profile)
                with connect_profile(profile, ledger) as transport:
                    result = run_task(args.task_dir, transport, resume=action == "resume",
                                      connection_profile=profile, workers=args.workers)
        print(json.dumps(result, sort_keys=True))
        return 0
    except TaskError as error:
        print(json.dumps(error.public_diagnostic(), sort_keys=True))
        return 2
    except Exception:
        print(json.dumps({"code": "TASK_FAILED", "phase": "task", "recoverable": False}))
        return 2
