"""Read-only certification binding of actual sources, wheel, nodes and logs."""

import hashlib
import json
import os
import re
import subprocess
import zipfile
from collections import Counter
from pathlib import Path

IMPLEMENTATION = "51ea23a4367cb8f069e930c5ab1882a0a00c8423"
TREE = "f0680c67652afc6cb04a236169ecc37bd425aa2d"
WORKER_SHA = "0b8bafc4f1388ad325be68721b421c2865c6bab7af339204af14a7dbea8d8d6a"


def git(*args):
    return subprocess.check_output(["git", *args])


def main():
    assert git("rev-parse", "HEAD").decode().strip() == IMPLEMENTATION
    assert git("rev-parse", "HEAD^{tree}").decode().strip() == TREE
    assert not git("diff", IMPLEMENTATION, "--", "src", "rust", "tests", "pyproject.toml")
    worker = Path(os.environ["SAKURAPOOL_RUST_WORKER"])
    assert hashlib.sha256(worker.read_bytes()).hexdigest() == WORKER_SHA
    evidence = Path(__file__).parent
    audit = json.loads((evidence / "fix3-full-collection.json").read_text(encoding="utf-8"))
    original, ordered = audit["original_nodeids"], audit["ordered_nodeids"]
    assert Counter(original) == Counter(ordered) and len(original) == len(ordered) == 523
    assert audit["nodeid_multiset_identical"]
    twohop = [node for node in ordered if node.split("::", 1)[0].endswith("test_p4_two_hop.py")]
    assert ordered[-len(twohop):] == twohop
    print(f"IMPLEMENTATION={IMPLEMENTATION}\nTREE={TREE}\nRELEASE_WORKER_SHA256={WORKER_SHA}")
    print(f"COLLECTION original=523 ordered=523 multiset_equal=True twohop_last={len(twohop)}")
    full = (evidence / "fix3-full-py312.log").read_text(encoding="utf-8")
    assert "521 passed, 2 skipped" in full and "EXIT_CODE=0" in full
    summaries = re.findall(r"^(PASSED|SKIPPED|FAILED) (tests[/\\].*)$", full, re.MULTILINE)
    passed_ids = [node for status, node in summaries if status == "PASSED"]
    assert len(passed_ids) == 521
    # Summary SKIPPED lines print source+reason, verbose lines retain full nodeid.
    verbose_skip = re.findall(r"^(tests/.*) SKIPPED \[", full, re.MULTILINE)
    assert len(verbose_skip) == 2
    assert Counter(passed_ids + verbose_skip) == Counter(ordered)
    print("OUTCOMES all 523 nodeids accounted for, no failure/deselection")
    for node in verbose_skip:
        print(f"SKIP_ID={node}")
    wheel = Path("D:/SakuraTool/SakuraPool-Fix3-20260930a/wheels/"
                 "sakurapool-0.1.0-py3-none-any.whl")
    sources = git("ls-tree", "-r", "--name-only", IMPLEMENTATION, "src/sakurapool")
    files = sources.decode().splitlines()
    compared = 0
    with zipfile.ZipFile(wheel) as archive:
        for source in files:
            if not source.endswith(".py"):
                continue
            actual = archive.read(source.removeprefix("src/"))
            expected = git("show", f"{IMPLEMENTATION}:{source}")
            # Windows working checkout CRLF is packaging-only, no source drift.
            assert actual.replace(b"\r\n", b"\n") == expected.replace(b"\r\n", b"\n"), source
            compared += 1
    print(f"WHEEL_SOURCE_MATCH files={compared} newline_normalization_only=True")
    raw = wheel.read_bytes()
    print(f"WHEEL bytes={len(raw)} sha256={hashlib.sha256(raw).hexdigest()}")
    print("CERTIFICATION_BINDING_OK")


if __name__ == "__main__":
    main()
