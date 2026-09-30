"""Run the COMPLETE suite with an auditable order-only collection plugin.

No PYTHONPATH change: importlib loads the evidence plugin by exact file path.
pytest's exit code is propagated unchanged. Product imports use the fresh
editable installation; binary source is the explicit release worker env var.
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest


def main():
    assert "PYTHONPATH" not in os.environ
    plugin_path = Path(__file__).with_name("fix3_collection_order.py")
    spec = importlib.util.spec_from_file_location("fix3_collection_order", plugin_path)
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    print(f"ORDER_ONLY_PLUGIN={plugin_path}")
    return pytest.main(["tests/", "-vv", "-rA", "--durations=15"], plugins=[plugin])


if __name__ == "__main__":
    sys.exit(main())
