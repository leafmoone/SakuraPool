"""Offline source-isolated packages from explicit frozen P2 inputs; no TAR scan."""

import argparse
import json
from pathlib import Path

from sakurapool.runtime.compiler import compile_runtime
from sakurapool.runtime.inventory import combine_inventories, load_p2_inventory
from sakurapool.storage.publication import build_publication, load_publication


def build_sources(asset, output):
    listed = json.loads((asset / "p2-roots.json").read_bytes())
    groups = {("danbooru", "danbooru_v3"): [], ("konachan", "konachan_v3"): []}
    identities = {key: set() for key in groups}
    for item in listed:
        root = Path(item)
        contract = json.loads((root / "INPUT.json").read_bytes())
        key = contract["adapter"]["source"], contract["adapter"]["dataset"]
        if key in groups:
            groups[key].append(root)
            for path in contract["inputs"]:
                identity = key[1], path
                if identity in identities[key]:
                    raise ValueError("duplicate source object")
                identities[key].add(identity)
    maps = {key: [] for key in groups}
    with (asset / "remote-map.jsonl").open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            identity = row["dataset_id"], row["object_path"]
            for key in groups:
                if identity in identities[key]:
                    maps[key].append(row)
    output.mkdir(parents=True, exist_ok=False)
    results = {}
    for key, roots in groups.items():
        if not roots:
            raise ValueError("empty source group")
        seen = {(row["dataset_id"], row["object_path"]) for row in maps[key]}
        if seen != identities[key] or len(seen) != len(maps[key]):
            raise ValueError("source remote map does not have exact coverage")
        directory = output / key[0]
        directory.mkdir()
        root_list = directory / "p2-roots.json"
        remote_map = directory / "remote-map.jsonl"
        root_list.write_text(
            json.dumps(
                {"format": "sakurapool-p2-root-list-v1", "roots": [str(root) for root in roots]}
            ),
            encoding="utf-8",
        )
        with remote_map.open("w", encoding="utf-8") as stream:
            for row in maps[key]:
                stream.write(json.dumps(row) + "\n")
        inventory = combine_inventories([load_p2_inventory(root) for root in roots])
        compile_runtime(inventory, directory / "runtime")
        build_publication(directory / "runtime", root_list, remote_map, directory / "publication")
        with load_publication(directory / "publication", full_verify=True) as publication:
            results[key[0]] = {
                "roots": len(roots),
                "objects": len(seen),
                "manifest": publication.manifest,
            }
        print(key[0], "FULL_VERIFIED", len(roots), len(seen), flush=True)
    (output / "build-results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build_sources(args.asset, args.output)
