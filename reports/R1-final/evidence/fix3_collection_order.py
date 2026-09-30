"""Audit-only Final Fix3 collection ordering, explicitly authorized by root.

No deselection, fixture mutation, result suppression, or budget changes. Old
mkdtemp-based two-hop tests grow the FIXED physical work root; place them last.
Save original/ordered full nodeid lists and assert exact Counter equality.
"""

import json
import os
from collections import Counter
from pathlib import Path

import pytest


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    original = [item.nodeid for item in items]
    items.sort(key=lambda item: item.nodeid.split("::", 1)[0].endswith("test_p4_two_hop.py"))
    ordered = [item.nodeid for item in items]
    identical = Counter(original) == Counter(ordered)
    report = {
        "reason": "authorized ordering only: fixed-root mkdtemp ledger tests last; no cleanup",
        "original_nodeids": original,
        "ordered_nodeids": ordered,
        "nodeid_multiset_identical": identical,
        "original_count": len(original),
        "ordered_count": len(ordered),
    }
    output = Path(os.environ["FIX3_COLLECTION_AUDIT"])
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    if not identical:
        raise pytest.UsageError("Final Fix3 nodeid multiset changed")
    print(f"\nFIX3_COLLECTION original={len(original)} ordered={len(ordered)} "
          f"multiset_equal={identical}")
    print(f"FIX3_COLLECTION_AUDIT={output}")
