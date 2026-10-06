"""Explicit format CAS updates; no output quota or persistent consumption."""

from ..image_formats import image_extensions
from .plan import canonical
from .store import TaskDB, TaskError


def update_task(directory, *, expected_settings_version, extensions):
    if type(expected_settings_version) is not int or not 0 <= expected_settings_version < (1 << 63):
        raise TaskError("SETTINGS_VERSION_INVALID", "update")
    try:
        selected = image_extensions(extensions)
    except ValueError:
        raise TaskError("IMAGE_EXTENSIONS_INVALID", "update") from None
    with TaskDB(directory) as task, task.runner_lock(), task.transaction() as db:
        task.validate_plan()
        old = task.settings_version
        if old == (1 << 63) - 1:
            raise TaskError("SETTINGS_VERSION_EXHAUSTED", "update")
        if old != expected_settings_version:
            raise TaskError("SETTINGS_VERSION_CONFLICT", "update")
        if db.execute("SELECT 1 FROM items WHERE state='IN_PROGRESS' LIMIT 1").fetchone():
            raise TaskError("RUNNER_BUSY", "update")
        if not set(task.image_extensions).issubset(selected):
            raise TaskError("IMAGE_EXTENSIONS_REMOVAL", "update")
        new = {"settings_version": old + 1, "image_extensions": list(selected)}
        for key, value in new.items():
            db.execute("UPDATE meta SET value=? WHERE key=?", (canonical(value).decode(), key))
    return new
