"""Explicit CAS updates, preserving frozen selection and cumulative delivery accounting."""
from ..image_formats import image_extensions
from ..storage.budget import BudgetCorrupt, BudgetExceeded
from .context import validate_ledger
from .plan import canonical
from .store import TaskDB, TaskError


def update_task(directory, ledger, *, expected_settings_version,
                max_output_bytes=None, extensions=None):
    if type(expected_settings_version) is not int or not 0 <= expected_settings_version < (1 << 63):
        raise TaskError("SETTINGS_VERSION_INVALID", "update")
    if max_output_bytes is None and extensions is None:
        raise TaskError("SETTINGS_UPDATE_EMPTY", "update")
    if max_output_bytes is not None and (
        type(max_output_bytes) is not int or not 0 <= max_output_bytes < (1 << 63)
    ):
        raise TaskError("OUTPUT_BUDGET_INVALID", "update")
    try:
        selected = image_extensions(extensions) if extensions is not None else None
    except ValueError:
        raise TaskError("IMAGE_EXTENSIONS_INVALID", "update") from None
    with TaskDB(directory) as task, task.runner_lock():
        validate_ledger(task, ledger)
        required = task.capacity.task_journal_bytes + max(
            0, task.capacity.task_db_bytes - (task.directory / 'task.sqlite').stat().st_size
        ) + 16384
        try:
            with ledger.quiescent_disk_operation(required), task.transaction() as db:
                task.validate_plan()
                old = {"settings_version": task.settings_version,
                       "image_extensions": list(task.image_extensions),
                       "max_output_bytes": task.meta("max_output_bytes")}
                if old["settings_version"] == (1 << 63) - 1:
                    raise TaskError("SETTINGS_VERSION_EXHAUSTED", "update")
                if old["settings_version"] != expected_settings_version:
                    raise TaskError("SETTINGS_VERSION_CONFLICT", "update")
                if db.execute(
                    "SELECT 1 FROM items WHERE state='IN_PROGRESS' "
                    "OR accounting='UNKNOWN' LIMIT 1"
                ).fetchone():
                    raise TaskError("ACCOUNTING_BUSY", "update")
                if selected is not None and not set(old["image_extensions"]).issubset(selected):
                    raise TaskError("IMAGE_EXTENSIONS_REMOVAL", "update")
                budget = old["max_output_bytes"] if max_output_bytes is None else max_output_bytes
                if budget < task.meta("confirmed_output_bytes"):
                    raise TaskError("OUTPUT_BUDGET_BELOW_CONFIRMED", "update")
                new = {"settings_version": expected_settings_version + 1,
                       "image_extensions": list(selected or old["image_extensions"]),
                       "max_output_bytes": budget}
                db.execute("CREATE TABLE IF NOT EXISTS settings_events("
                           "version INTEGER PRIMARY KEY, old TEXT NOT NULL, new TEXT NOT NULL)")
                db.execute("INSERT INTO settings_events VALUES(?,?,?)",
                           (new["settings_version"], canonical(old).decode(),
                            canonical(new).decode()))
                for key, value in new.items():
                    db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                               (key, canonical(value).decode()))
        except BudgetCorrupt as error:
            if str(error) != "metadata reservation settlement unknown":
                raise
            raise TaskError("TASK_RESOURCE_SETTLEMENT_UNKNOWN", "update") from None
        except BudgetExceeded as error:
            code = "ACCOUNTING_BUSY" if str(error) == "ACCOUNTING_BUSY" else "RESOURCE_BLOCKED"
            raise TaskError(code, "update", recoverable=True) from None
        return new
