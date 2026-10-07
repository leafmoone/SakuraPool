# SakuraPool

SakuraPool builds committed TAR indexes, compiles read-only query runtimes, and
uses source-separated publications for verified selective image/JSON retrieval.
It does not decode images or run models. Ordinary use needs a complete local
publication, not original TAR downloads or a new indexing run.

## Deploy the product

Use Python 3.10+ with the pinned dependencies; the current verified Windows
interpreter is Python 3.13. PyArrow 18.1.0 / NumPy 2.2.6 do not provide Windows
CPython 3.14 wheels. A newer version number alone does not establish compatibility.
The Python wheel contains the Python package, not a prebuilt Rust executable.
Build the worker from this repository or the source distribution with the committed
Cargo.lock. The Windows worker build/hello was verified with
Rust 1.98.1 GNU and MSYS2 UCRT64; native Debian 13 verification is recorded below. `rust/rust-toolchain.toml` pins
`1.98.1-x86_64-pc-windows-gnu`; it is not a native Linux/macOS toolchain pin.
Choose the toolchain explicitly below: rustup selects directory pins from the
launch directory and its parents, not from Cargo's `--manifest-path`.

```console
git clone --branch main https://github.com/leafmoone/SakuraPool.git
cd SakuraPool
python -m venv .venv
# Activate .venv using your shell, then:
python -m pip install '.[remote]'
```

PowerShell worker build from the repository root (the verified Windows GNU
configuration; project-local output, no inherited shared Cargo target). Install
the pinned toolchain first if it is not already present; MSYS2 UCRT64 must provide
the native compiler/linker tools, including `gcc` and `dlltool`.

```powershell
rustup toolchain install 1.98.1-x86_64-pc-windows-gnu --profile minimal
Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
# Example if MSYS2 UCRT64 is installed at the conventional location:
$env:PATH = "C:\msys64\ucrt64\bin;" + $env:PATH
cargo +1.98.1-x86_64-pc-windows-gnu build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker --target-dir rust/target
$worker = (Resolve-Path rust/target/release/sakurapool-worker.exe).Path
sakura --version
```

The generic POSIX guidance below needs the actual native host triple.
Debian 13 x86-64 build/hello and offline smoke checks are verified below;
macOS and real authenticated image retrieval remain unverified.
From the repository root, install Rust 1.98.1 for your actual native host and
select it explicitly as `cargo +1.98.1-<native-host-triple>`. Replace the placeholder
with your Linux/macOS host triple; do not enter `rust/` and rely on its Windows
host override. The host needs its native linker, compiler and build dependencies;
a cross-compilation `--target` flag is not a substitute for a native-host toolchain.

```console
# Replace <native-host-triple> before running these commands.
toolchain='1.98.1-<native-host-triple>'
rustup toolchain install "$toolchain" --profile minimal
env -u CARGO_TARGET_DIR cargo "+$toolchain" build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker --target-dir rust/target
```

On POSIX the executable path would be `rust/target/release/sakurapool-worker`;
on Windows it has the `.exe` suffix. Keep its absolute path for the private
connection profile below. Source distributions carry the Rust sources/lockfile;
compiling the worker does not require image data or ModelScope credentials.
After the matching host toolchain and native build tools are installed, a
prepopulated Cargo dependency cache permits adding `--offline` to the build command.
`--offline` does not install a missing toolchain, linker or dependency cache.

## Debian 13 native Linux deployment and validation

Validated on Debian GNU/Linux 13.6, x86-64, Python 3.12.14, Rust
`1.98.1-x86_64-unknown-linux-gnu`, against main commit
`84f381e0b9685f7c832ebd31d567283038e06b1c` on 2026-10-07.
The release worker built successfully with the committed Cargo.lock. This is
build/CLI/offline verification, **not a completed real-image throughput benchmark**.

Install Python with venv support, a native C compiler/linker, and Rust from their
official distributions. Then, from the repository root (Bash):

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install '.[remote]'
python -m pip check
rustup toolchain install 1.98.1-x86_64-unknown-linux-gnu --profile minimal
env -u CARGO_TARGET_DIR cargo +1.98.1-x86_64-unknown-linux-gnu build \
  --manifest-path rust/Cargo.toml --locked --release \
  --bin sakurapool-worker --target-dir rust/target
worker="$(pwd)/rust/target/release/sakurapool-worker"
sakura --version
sakura config validate --config examples/index-config.json
sakura validate examples/query.json
sakura evaluate examples/query.json examples/rows.json
```

Keep the explicit native toolchain: `rust/rust-toolchain.toml` still pins a
Windows host. The Python wheel alone does not supply the worker. If your managed
Linux environment restricts home-directory writes, set `CARGO_HOME` and
`RUSTUP_HOME` to writable project-local directories before installing and building.
Do not disable network/access controls to work around a blocked provider.

The Linux checks passed: pinned Python installation and `pip check`; CLI version,
configuration and example query/evaluation; native release build; protocol-2 hello
with `production_download_lightweight_v1` and exact execution-limit echo; a local
three-record TAR index with computed image hashes; committed-index reuse; runtime
compile, full verification and query; and rejection of zero workers. The local
TAR is a synthetic functional fixture, not Danbooru data or a speed test.
`cargo test --locked --release` completed but contains **zero Rust tests**.
The `dot` branch adds 13 focused Python proxy/TLS/credential-isolation tests;
these pass with `python -m unittest discover -s tests -v`. This is focused
coverage, not the complete development suite on `dev`.

### Reproducible Danbooru / six-worker benchmark

“worker6” means `--workers 6`; the source query is `danbooru`. Install the complete
pinned `sources/danbooru` publication using the acquisition example below (set
`source = "danbooru"`), preserve its paths, and fully verify it. This source's
12 index files total 2,978,602,701 bytes; do not count index installation as image
throughput. Public index access does not authorize original-image access.

```bash
sakura publication verify local-index/sources/danbooru --full
printf '%s\n' '{"sources":["danbooru"]}' > danbooru-query.json
python tools/prepare_linux_benchmark.py \
  --publication local-index/sources/danbooru --output benchmark-selection \
  --seed linux-danbooru-1000-v1
sakura task create --publication local-index/sources/danbooru \
  --query benchmark-selection/danbooru-query.json --selection records \
  --records benchmark-selection/danbooru-records.json --task-dir DANBOORU_1000
sakura task inspect DANBOORU_1000
# Use a private task profile with the absolute native worker path.
# Supply authorized ModelScope credentials through your environment's secure flow.
/usr/bin/time -v sakura task run DANBOORU_1000 --profile TASK_PROFILE.json --workers 6
sakura task inspect DANBOORU_1000
sakura task export DANBOORU_1000 --manifest DANBOORU_1000/manifest.jsonl
```

These are follow-on commands, **not yet a claim that the 1,000-image run passed**.
The helper full-verifies the publication, deterministically ranks eligible TARs
and records by the seed, and freezes 1,000 records with unique indexed image
hashes from six TARs. The per-TAR counts are 167/167/167/167/166/166, interleaved
round-robin to exercise six lanes and cache reuse. This is a locality-friendly
workload, not a representative global-random Danbooru sample.
Use fresh selection/task/output directories, retain the generated selection.json
and record IDs, and check all deliveries against indexed SHA256 and exact sizes.
Report unique record IDs and unique content SHA256 separately: 1,000 selected
records do not necessarily mean 1,000 unique image contents. Record failures,
retries, elapsed time, delivered image bytes, metadata bytes if requested, and
observed active lanes/distinct TARs. Do not silently drop failed or duplicate
records or count a resumed cached file as newly downloaded bytes.

Report image goodput as verified newly downloaded image bytes divided by timed
run seconds (MiB/s = bytes / seconds / 2**20; Mbit/s = bytes * 8 / seconds / 1e6).
It is different from wire throughput, which includes metadata, headers, retries
and transport overhead. Report bandwidth utilization only with a verified,
applicable bandwidth-capacity denominator; nominal NIC speed, package-install
speed and unrelated speed tests are not that denominator.

Six requested workers do not guarantee six active transfers. Requests for the
same TAR are deliberately gated. A seeded sample helps spread work across TARs;
inspect the actual distribution. The coordinator's 2L candidate window (12 for
six lanes) can also leave lanes idle behind a same-TAR prefix even when later
independent TARs exist. Compare the same frozen record set before tuning worker
count or scheduling, and preserve conditional proofs, retries, hashes and
no-overwrite guarantees. No throughput improvement has been measured here.

**Validation status (2026-10-07, in progress):** authorized access now works.
The project control plane and native Rust worker have passed live metadata,
positive Range and negative-condition probes. The public index installation
and real 1,000-image run are still being completed; no final image throughput
or bandwidth-utilization result is claimed at this checkpoint. The earlier
anonymous original-repository requests returned provider authentication code
`10020000500`; credentials remain outside source, profiles, logs and README.

### Environment proxies and TLS on Linux

Production HTTP requests now honor ordinary `HTTP_PROXY`, `HTTPS_PROXY`,
`ALL_PROXY` and `NO_PROXY` environment configuration, including the lower-case
forms supported by Requests/reqwest. No separate SakuraPool proxy opt-in is
required. Python keeps `trust_env=False` only to prevent unrelated `.netrc`
credential discovery; it explicitly resolves proxies per request URL. The fresh
CDN session separately resolves its proxy and never receives the origin bearer
or session cookie. Redirect validation and no automatic redirect following remain.
Synthetic loopback tests still bypass proxies, even for mixed-case HTTP schemes.

Python uses `REQUESTS_CA_BUNDLE` / `CURL_CA_BUNDLE` when configured, otherwise
an available system CA file/directory (then Requests' default). Rust uses native
system roots through reqwest's `rustls-tls-native-roots` feature. Standard native
root settings such as `SSL_CERT_FILE` apply to the Rust loader. If configuring a
custom CA location, make both clients' trust settings consistent. HTTPS verification
is never disabled. No new trust anchor was installed in the validated environment;
its already-provisioned system CA is used for the platform's managed egress.
As with other applications using system trust, a trusted TLS-inspecting proxy
can inspect traffic. Secrets and signed redirect URLs must not be put in logs.

## Public source indexes and explicit installation

Public index repository: [leafmoone/SakuraPool](https://modelscope.cn/datasets/leafmoone/SakuraPool).
Pin release **`8d0aab1697b57288caf13b4c18c08fb2fa401518`**. It covers **715 committed
partitions / 22,792 TAR objects / 26,430,011 retained records**, verified from the
COMMIT/Parquet contracts and exact fixed provider path/size/SHA metadata, not an
estimated 22M/30M/40M or a claim of unique image content.

| Source | Partitions | TAR objects | Records | Local index bytes |
|---|---:|---:|---:|---:|
| anime_pictures | 32 | 1,000 | 642,954 | 169,066,240 |
| bangumi | 94 | 3,000 | 4,168,774 | 836,745,004 |
| danbooru | 361 | 11,552 | 11,132,809 | 2,978,602,701 |
| gamecg | 8 | 240 | 5,177,447 | 1,212,210,257 |
| konachan | 32 | 1,000 | 318,760 | 80,221,647 |
| yande | 94 | 3,000 | 1,124,564 | 245,637,771 |
| zerochan | 94 | 3,000 | 3,864,703 | 935,541,500 |

Each source has an independent runtime and publication, with 12 required files.
Download only the chosen source, preserve every relative path, and do not mix
sources/snapshots. `catalog.json` is the public file/source catalog. The release
combines 612 partitions from `leafmoone/sakurapool-index` at
`f94469c4b5c8b5d082c5c34f116a2286a633968b` and 103 disjoint partitions from
`leafmoone/sakurapool-index-checkpoint-20261003t111628z` at
`ec49db9baff7c1dc10457e5012e6b8f3be1ae64a`.

Index acquisition is explicit; SakuraPool has no automatic discovery/installer.
The following standalone example uses the verified public dataset file API and
no credential. Change `source` to another table entry and use a fresh destination;
it downloads index files only, not images. Install `.[remote]` for `requests`.

```python
from pathlib import Path, PurePosixPath
import os
import ssl
import requests

repo = "leafmoone/SakuraPool"
revision = "8d0aab1697b57288caf13b4c18c08fb2fa401518"
source = "konachan"
root = Path("local-index")
root.mkdir(exist_ok=False)
url = f"https://modelscope.cn/api/v1/datasets/{repo}/repo"
with requests.Session() as session:
    session.trust_env = False  # Do not discover unrelated .netrc credentials.
    session.proxies = requests.utils.get_environ_proxies(url)
    system_ca = ssl.get_default_verify_paths()
    session.verify = (os.environ.get("REQUESTS_CA_BUNDLE")
                      or os.environ.get("CURL_CA_BUNDLE")
                      or system_ca.cafile or system_ca.capath or True)
    response = session.get(url, params={"Revision": revision, "FilePath": "catalog.json"},
                           timeout=(10, 60))
    response.raise_for_status()
    catalog = response.json()
    entry = next(item for item in catalog["sources"] if item["source"] == source)
    for item in entry["files"]:
        relative = PurePosixPath(entry["path"]) / item["path"]
        if relative.is_absolute() or ".." in relative.parts or "\\" in str(relative):
            raise ValueError("invalid catalog path")
        target = root / str(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        size = 0
        with session.get(url, params={"Revision": revision, "FilePath": str(relative)},
                         stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            with target.open("xb") as output:
                for chunk in response.iter_content(1024 * 1024):
                    size += len(chunk)
                    if size > item["bytes"]:
                        raise ValueError("index file exceeds declared size")
                    output.write(chunk)
        if size != item["bytes"]:
            raise ValueError("index file is truncated")
print(root / entry["path"])
```

Then fully verify the downloaded publication; its root is the selected source
folder, **not** `local-index` itself:

```console
sakura publication verify local-index/sources/konachan --full
```

Public index access does **not** grant access or rights to the original images.
Image locators remain `leafmoone/webdataset_danbooru_v3` at
`73306f1dc5459238710f477b376c36da997d020c`. That repository is marked private;
original-repository authorization and credentials are required. Anonymous detail
metadata is readable, but it is not proof of anonymous image access. Apache-2.0
covers the distributed index metadata, not original-image or third-party rights.
No image TAR or private checkpoint management file is mirrored in the public index.

### Query one source and create an image-download task

Save `{"sources":["konachan"]}` as `source-query.json`. A source query is a query
JSON field, not a `task create --source` option. Task creation is local/offline:

```console
sakura task create --publication local-index/sources/konachan --query source-query.json --selection first --limit 3 --task-dir TASK
sakura task inspect TASK
```

For later retrieval, save a **private** `TASK_PROFILE.json` with your built worker
path and a credential reference (do not put the token itself in this JSON):

```json
{
  "format": "sakurapool-task-connection-v1",
  "origin": "https://modelscope.cn",
  "repositories": ["leafmoone/webdataset_danbooru_v3"],
  "worker": "/absolute/path/to/sakurapool-worker",
  "credential_ref": {"env": "MODELSCOPE_API_TOKEN"}
}
```

On Windows the worker path must be your real absolute `.exe` path, escaped for
JSON. Set `MODELSCOPE_API_TOKEN` privately to a credential with original-repository
access, then explicitly start the image task:

```console
sakura task run TASK --profile TASK_PROFILE.json --workers 1
sakura task export TASK --manifest TASK/manifest.jsonl
```

This example selects three images; creation/inspection do not download them.
`publication fetch` downloads selected image records, not index packages. Larger
worker counts are explicit choices, not automatic tuning or throughput promises.

## Lightweight downloads (current contract)

New downloads use **TaskDB v4**, frozen flat-output naming, and **workspace manifest v2**. The production
path does not instantiate `BudgetLedger`, read/write budget slots, recursively
scan disk for quota admission, or persist request/body/metadata/saved-byte
consumption. There is no cumulative request, bandwidth, saved-count, disk-output,
or task-output quota. `--max-output-bytes` and workspace policy updates have been
removed from the download CLI. Historical ledger files are never reset or settled.

Only light task state is durable: frozen query/selection/seed, publication and
plan identity, current operation/phase, owned temporary-file identity, verified
file receipt, bounded retry lifecycle, and a sanitized failure diagnostic. An
item failure and its diagnostic commit together with task `FAILED`. Use
`task inspect TASK --failure-seq SEQ` to read it without changing the task.
Diagnostics contain fixed codes/phases, HTTP status and member/chunk location;
never exception messages, URLs, paths, tokens, headers or cumulative consumption.

New workers negotiate protocol **2** with same-instance capability
`production_download_lightweight_v1` and exact technical execution-limit echo.
An old worker or mismatched capability/limits fails before provider requests.
Build a matching worker with an explicit project target directory and update a
connection description deliberately; old profiles/binaries are not auto-rewritten.
Protocol 1 remains for historical indexing/administration operations, not a
hidden legacy download switch. Request-ID deduplication remains bounded at 256;
rotation clears conditional proofs and revalidates them before further Range IO.

The following remain technical safety/format boundaries, not consumption quotas:
per-request chunk/header/RPC buffers, selection heap and
SQLite/JSON parser bounds, exact indexed extents and image SHA, bounded metadata
validation, trusted origin/redirect policy, conditional positive/negative proof,
finite Origin retry (at most 3) plus CDN hop, owned temporary files and atomic
no-overwrite **per-file** publication. An image plus JSON is not pair-atomic:
only the verified receipt/DONE state and exported manifest mark a complete delivery.
Large images may stream over several configured chunks.
No change implies image decoding or conversion.

Resume never reselects records. A published receipt must match task/operation,
file/directory identities and indexed image SHA before `DONE/VERIFIED`; valid
published output is reused, not redownloaded. Crashed `IN_PROGRESS` can become
`READY` only after claimed-before-network or exact owned-temp cleanup evidence.
Unknown/replaced output and ambiguous protocol failures remain `BLOCKED`, not
blindly retried. Classified transient, unpublished failures require explicit
resume; at most two recovery retries survive process restarts. This is not an
end-to-end exactly-once or Windows directory power-loss guarantee.

Existing TaskDB v3 record-directory archives retain **read-only DB inspection and
receipt-verified export compatibility**. Export creates a new manifest in that task
domain: it is not a zero-write operation on the entire domain. All v3 execution,
resume, pause/cancel and settings updates now explicitly fail
`LEGACY_TASK_MIGRATION_REQUIRED` before credentials or writable SQLite. This is an
explicit format4 execution-contract switch, not automatic migration. Completed v3
images/record directories and already generated manifests are not renamed or changed.

Existing TaskDB v1/v2 archives allow **read-only inspection only**. Export writes
an artifact, so legacy export is explicitly rejected as well: every action except
inspect fails `LEGACY_TASK_MIGRATION_REQUIRED` before publication loading, output
creation, writable SQLite/journal or credential handling. This does not authorize
export to an external path, copying or migration. Migration is not automatic:
confirmed deliveries need revalidation, uncertain items need isolation, and old
UNKNOWN/pending accounting must remain archived. No migration or production
resume is authorized by these implementation changes.

## Flat filenames and recoverable publication

New task outputs are directly under `output`, without record-ID directories. Task
creation accepts `--filename-template` (default `{tag}_{index}`) and optional
`--filename-prefix` replacing the tag label. The only supported fields are `{tag}`
and `{index}`; index must occur exactly once, without format specs, conversions or
attribute lookup. `index` is frozen selection `seq + 1`, never completion order.
For a `1girl` query the outputs are `output/1girl_1.jpg`, `output/1girl_2.png`, etc.;
requested and available metadata uses the same stem, such as `1girl_1.json`.
Suffixes come from the actual indexed format; templates cannot specify extensions.

Positive all/any tags and any-of branches are deterministically sorted/deduplicated
and joined with underscores (qualified tags include namespace). Negative tags do
not affect naming; no positive tag means `image`. Generated labels are NFC-normalized
and Windows-forbidden characters become underscores. Explicit prefixes/template
literals reject unsafe characters, controls, separators/traversal, reserved Windows
names and terminal spaces/dots. Length limits are 240 UTF-8 bytes /180 UTF-16 units
per basename and 240 UTF-16 units for the absolute output path. Long labels fail
rather than silently truncate; choose an explicit short prefix. NFC-casefold stem
uniqueness is checked at task freeze, independent of extension. All targets use
no-overwrite publication. Filename policy/config and per-item stems are part of the
frozen header/plan; format settings updates cannot rename them.

```text
sakura task create --workspace WORKSPACE --publication PUB --query QUERY.json --selection first --limit 3 --metadata --filename-template "{tag}_{index}" --filename-prefix batch --task-dir WORKSPACE/tasks/TASK
sakura task run WORKSPACE/tasks/TASK --profile PROFILE.json --workers 6
```

The same publisher is used by direct `publication fetch` (default `image_1`, explicit
`--filename-index`, template/prefix options) and `PublicationSession.fetch`, whose
caller must supply `filename_index`. Direct/session callers select stable indexes;
there is no automatic completion-order counter.

A private identity-owned stage holds verified/fsynced bytes. PREPARED commits the
exact source-to-final member map, root/stage/file identities and hashes;
PUBLISH_INTENT commits before any no-replace rename. Per-member SQL acknowledgements
and final verification precede PUBLISHED/DONE. A task restart can finish an interrupted
pair only from that durable intent plus exact original file identity. Matching
names/content alone cannot authorize adoption, deletion or overwrite. Missing,
replaced, ambiguous or moved-back acknowledged files remain BLOCKED and preserved.
The shared output directory may contain other records/user files; recovery checks
only the operation's mapping and private stage, not exclusive ownership of output.

There may be a visible image before its JSON. Direct/session calls without TaskDB
preserve partial data on failure and do not promise automatic restart/adoption.
Unsupported no-replace platforms fail closed; there is no fallback to overwriting
rename and no Windows power-loss/exactly-once promise. No consumption ledger is added.

## Raw image delivery settings

Publication/task downloads support registered `.jpg`, `.jpeg`, `.png`, `.webp`,
`.avif`, and `.gif` formats. The default remains the first five; GIF is opt-in.
`publication fetch` and `task create` accept `--image-extensions` as comma-separated
suffixes. Bytes are verified and delivered unchanged; no decoding or conversion
is performed. Unknown formats and unsafe filenames are rejected. Index adapter
`image_extensions` controls scanning and does not automatically enable download
formats.

New v4 tasks can explicitly expand their format selection without reselecting records:

```text
sakura task inspect /workspace/tasks/task
sakura task update /workspace/tasks/task --expected-settings-version 0 --image-extensions .jpg,.jpeg,.png,.webp,.avif,.gif
```

Use the actual `settings_version` from inspect. New tasks start at version zero
with the original five defaults. Updates use compare-and-swap, reject format
removal and active runners/items, and commit the latest format/version in one
transaction. Frozen selection/seed/plan and verified deliveries are unchanged.
There is no output ceiling, ledger lease or consumption audit to update.


The serial Python publication reader is:
`from sakurapool.storage.publication_session import PublicationSession`.
A session fully verifies once, serves multiple record fetches and closes its bounded
binding cache. It owns the publication handle, not the caller's transport.
PublicationSession itself is serial and same-thread: no cross-thread SQLite use.
The task coordinator separately supports bounded concurrent lanes; this is not a
concurrent scheduler inside a shared PublicationSession.
Default task workers=1; `--workers` and the API accept
any strictly positive integer, without an enumeration or configured maximum.
Actual concurrency depends on ready work, object distribution and available OS
resources. Earlier P3/P4 CLI examples below are historical/admin interfaces,
not the default task workflow.

### Speed and concurrency limits

The previous 1/2/4/6 choices and estimated 512 MiB aggregate-memory admission are
removed by explicit user contract change. Zero, negative and non-integer workers
(including API booleans) are invalid; default remains 1. No machine-RAM detection,
automatic worker increase or replacement memory ceiling is introduced. Choosing
large concurrency can exhaust real machine resources; successful execution or
throughput growth is not guaranteed.

Let L be the smaller of requested workers and currently ready records. Lanes are
created lazily for independent usable work, not eagerly for the full input. Active
operations are bounded by L, candidate/claim lookahead and event envelopes by 2L,
and completion envelopes by L. SQL lookahead is clipped to actual READY rows before
binding LIMIT, so very large positive Python integers do not overflow SQLite or
impose a hidden input maximum. There is no fixed 12-record candidate ceiling.
Simultaneous requests to the same TAR remain gated, and per-request chunk/header/RPC,
proof-cache, finite retry, integrity and no-overwrite boundaries are unchanged.

## Workspace and implementation boundaries

```text
sakura workspace init WORKSPACE
sakura workspace inspect WORKSPACE
sakura doctor --workspace WORKSPACE --profile PROFILE.json
sakura task create --workspace WORKSPACE --publication PUB --query QUERY.json --selection first --limit 3 --metadata --task-dir WORKSPACE/tasks/TASK
```

New workspace manifest v2 freezes physical-domain identity and technical capacity;
TaskDB v4 binds the workspace, exact frozen plan and filename mapping. `workspace init --config`
accepts only `capacity`, not a download resource policy. `workspace inspect` reads
legacy workspace identity without opening its ledger. Legacy-task migration is
explicitly separate and is not implemented automatically.

Metadata is validated as a JSON object without materializing its object graph: strict
UTF-8, no BOM, NaN or Infinity, depth at most 128 and at most 1,000,000 nodes.
These are validator implementation boundaries, not adjustable metadata byte capacity.
Duplicate keys and escaped unpaired surrogates remain accepted. Flagged empty metadata
is invalid. This is intentionally narrower than every input accepted by `json.loads`.
The fixed validator working-set allowance is 8,320 bytes; validation failure
prevents publication and preserves unrecognized temporary files.

RPC capacity is negotiated, with a separate 64 KiB bootstrap and bounded queue/parser.
The pinned reqwest/hyper HTTP/1 implementation has a 417,792-byte incomplete-head
threshold and an independent 100-field limit. Configured decoded header bytes above
417,760 are rejected before worker start. The boundary test reaches 417,760 with a
canonical HTTP/1.1 206 status line; extra wire whitespace or a longer reason phrase
can hit the underlying parser boundary earlier. It is not a universal response guarantee.

Download lanes have no estimated aggregate-memory admission or worker-count
maximum. This does not remove the independent per-request protocol/parser/header
and chunk capacities described above, nor image/metadata integrity checks. There
is no promise that a chosen worker count fits physical RAM; actual OS resource
failures remain possible. Historical protocol allocation factors are not a new
aggregate download ceiling.

## Task API (recoverable task workflow)

Task creation freezes the publication content/snapshot identity, normalized P3
query and deterministic selection into a SQLite TaskDB. Resume uses those exact
seq rows, never repeats selection or sampling. `task_id` distinguishes instances;
`plan_digest` binds the reproducible plan. Modes are `all`, `first`, explicit
`records`, and versioned SHA256 top-K `sample` with an explicit seed.

```text
sakura task create --publication PUB --query QUERY.json --selection first --limit 3 --task-dir TASK
sakura task create --publication PUB --query QUERY.json --selection sample --limit 3 --seed seed --task-dir TASK
sakura task inspect TASK
sakura task run TASK --profile TASK_PROFILE.json
sakura task pause TASK
sakura task cancel TASK
sakura task resume TASK --profile TASK_PROFILE.json
sakura task export TASK --manifest OUTPUT.jsonl
```

Creation/inspection are local: no HTTP or token reading. Task/output/export paths
must be contained in their validated workspace physical domain; publication can be read
outside it. Workspace-bound tasks support automatic identity discovery; standalone
new tasks are also lightweight. TaskDB uses DELETE journal and FULL synchronization with short explicit
transactions, plus a real single-runner OS file lock. One serial session fully
verifies the publication once per runner process. Pause/cancel CLI returns a
**requested** state: the bounded current item finishes before another is claimed.
Cancellation preserves deliveries and requires explicit resume. With concurrent
lanes, new claims stop when pause/cancel is observed; already-admitted active items
drain before the run returns. Same-object affinity does not promise multiple
concurrent streams from a single TAR.

Task connection profiles are separate from strict old single-object profiles:
`{"format":"sakurapool-task-connection-v1","origin":"https://modelscope.cn",
"repositories":["owner/dataset"],"worker":"ABSOLUTE_WORKER_PATH"}`.
Optional `credential_ref` is `{"env":"MODELSCOPE_API_TOKEN"}` or a bounded
`{"file":"ABSOLUTE_TOKEN_FILE"}` reference, not an embedded credential.
Object paths/revisions/digests come from the pinned publication, not this profile.

Successful durable delivery receipts recover without redownloading. Unknown
output is preserved and blocked; neither filename existence nor absence of a
historic lease proves success. There is no end-to-end exactly-once claim.
New v4 tasks and receipt-verified v3 archives can export; legacy v1/v2 export
requires migration and is currently rejected without writing. Exports stream only
receipt-verified deliveries with source/plan identity,
relative task paths and delivery hashes, without image copies or archives.
The manifest must be directly inside TASK (for example TASK/subset.jsonl), so
its relative `output/...` paths resolve relative to the manifest's directory;
a different export base is explicitly rejected.
Legacy binary ledger formats remain archives/admin compatibility and are not
used by new downloads. No exact per-task network consumption is recorded or
inferred from historical global counters.

Default technical task capacities (configurable, not consumption quotas): 100,000 frozen entries, 10,000 in-memory sample
heap entries, 512-record identity batches, 32 MiB TaskDB, 33 MiB journal allowance,
64 KiB plan header and 32 MiB exported manifest. SQLite temp work uses memory;
the maximum DB page count is derived from its actual page size. Windows file/
SQLite fsync is used; portable Windows directory power-loss durability is not
claimed. Configure image capacity for larger images while keeping bounded chunks.
Bounded task concurrency is available; automatic legacy migration is not implemented.
Production-scale validation is tracked separately.

## Publication v2 (runtime-first distribution)

Publication v2 is separate from the historical package v1. It preserves durable
P2 format 4 and runtime format 2; its default distribution contains no durable
partitions. Maintainers retain P2 separately for audit and recompilation.

```text
sakura publication build --runtime RUNTIME --p2-list p2-roots.json --remote-map remote-map.jsonl --output NEW_PUBLICATION
sakura publication inspect NEW_PUBLICATION
sakura publication verify NEW_PUBLICATION --full
sakura publication fetch --publication NEW_PUBLICATION --record-id RECORD_ID --profile PROFILE --output P4_OUTPUT --metadata
```

The P2 list is `{"format":"sakurapool-p2-root-list-v1","roots":["absolute/path"]}`.
Each bounded JSONL remote-map row contains `dataset_id`, `endpoint`, `repo_id`,
`repo_type`, `revision_candidate`, `object_path`, `object_size`, and
`provider_sha256` (64 lowercase hex or null). Rows must exactly cover runtime
objects. Only provider SHA equal to the indexed whole-TAR SHA makes an object
fetchable. Null provider SHA keeps query/location usable but blocks remote fetch.

The publication contains a bounded `PUBLICATION.json`, pinned runtime snapshot,
read-only `remote_objects.sqlite`, and mmap `image_sha256.npy` (`V32`, indexed by
rid). Build requires reliable computed image SHA in every P2 sample. Fast open
checks structure and identity, not complete content hashes; full verify streams
all hashes and cross-checks catalog identities. Fetch requires a full-verified
publication, fresh exact provider lookup, then its own positive/negative
conditional binding before Rust Range reads. Neither live ETag/proof nor signed
URL is persisted. Publication storage may be outside the network work root;
fetch outputs use the same ledger-free streaming and integrity path as tasks.

Image mismatch prevents delivery. A crash after atomic rename may leave visible
output with an unfinished task operation; recovery verifies its durable receipt
before reuse, and no output is overwritten.
Synthetic million-record measurements are not a production capacity guarantee.

## Project workflow and boundaries

The explicit administrator flow is remote TAR indexing →
durable index → runtime compile/verify → versioned index publication. The user
flow is install/verify a published index → query → locate image/JSON → Range
retrieval → a small local dataset. Earlier P3 descriptions of remote scanning,
publication and retrieval as future work are historical; current explicit interfaces
are described above. The separately published fixed index scope is listed at the top.

Ordinary users do not need complete local TARs; LocalTarScanner is for local
inputs, testing and validation. Use the published complete source folders above;
do not search for old hfutils/CheeseChaser indexes as a prerequisite. New large
image scans/builds require separate authorization; naming an image repository does
not grant access, scanning or write permission.

The explicit Rust administrator pipeline has download-then-scan and
remote-stream-scan modes; see [R2 transport](docs/R2_TRANSPORT.md) for configuration
and whole-TAR spool/memory limits, and [local partition builder](docs/local-partition-builder.md)
for already-local TARs. These indexing/administration interfaces are separate from
ordinary lightweight retrieval; ordinary fetch requires a verified local publication
and never implicitly scans original archives.

## Installation and CLI

Python 3.10 or newer is required by the code and dependency minimums; there is no
project-imposed upper version bound or mandatory Python 3.12 certification policy.
Use the newest stable interpreter that actually installs and passes verification
with the pinned dependencies, not simply the highest version number. On Windows
x86-64, the current PyArrow 18.1.0 / NumPy 2.2.6 pins provide CPython 3.13 wheels
but not 3.14 wheels. Removing the metadata upper bound does not certify 3.14 or
permit silently upgrading those pins, transplanting Arrow, or using `--no-deps`.

Verification subprocesses use the invoking interpreter (`sys.executable`)
rather than a retired virtual-environment path. Development tests live on `dev`;
product-only `main` and release distributions intentionally exclude tests/reports/plans.

```console
python -m pip install '.[remote]'
sakura --version
sakura config validate --config examples/index-config.json
sakura index scan --config examples/index-config.json --dataset synthetic --input /local/tars --output /local/index
sakurapool validate examples/query.json
sakurapool evaluate examples/query.json examples/rows.json
```

`sakurapool` remains an alias. `index scan INPUT OUTPUT` remains supported. Input may
be one uncompressed TAR or a directory recursively containing TARs. Compression is
rejected by known extension, content magic, and the uncompressed TAR reader, including
compressed content disguised as `.tar`. Inputs/outputs must not overlap.

## P1_CONTRACT_CHANGE

**Original definition:** P1 implemented only `DEFAULT_SCHEMA(id, value, label)` and
query/model/schema registries. It did not implement the required ObjectRef, MemberRef,
RecordKey or namespaced tag contract. Initial P2 incorrectly defined an Object as an
image occurrence, hashed dataset/source/shard/member/extents with SHA256, and made
samples one-to-one with objects. Tags were bare strings.

**Problem:** one TAR contains many physical records; moving extents changed identity;
there were no JSON member extents or registry-defined tag namespace/origin/category.
P1's toy query schema is not evidence that these original domain requirements existed.

**New definition (on-disk schema version 4):** one Object is one TAR shard, identified
by a stable storage profile plus a content-bound object_id. Each successfully paired
physical sample is one samples row; invalid/unpaired candidates produce errors while
required missing metadata retains the sample row.
`RecordKey=(dataset_id, object_id, sample_path)`, where sample_path is the adapter's
full logical member stem, not an image byte offset. `record_id` is exactly:

```python
hashlib.blake2b(json.dumps(
    ["sakurapool-record-v1", dataset_id, object_id, sample_path],
    ensure_ascii=False, separators=(",", ":")
).encode("utf-8"), digest_size=16).hexdigest()
```

The object_path is a canonical relative POSIX path; object_id is content-bound and
includes the SHA256 object version. Canonical paths are nonempty and have no empty,
dot, dot-dot, backslash, or colon components. Noncanonical IDs are rejected, never silently mapped.
Original key fields are retained with every sample/annotation. Duplicate/colliding
keys fail without publishing that shard. This is physical-record identity, not dedup.
`records.ObjectRef` is a self-contained retrieval object descriptor with
`storage_id/object_id/object_path/object_size/object_version/validator/backend/repo_type/
archive_format/validator_kind/validator_strength`; `archive_format` is keyword-only to
preserve the pre-existing positional ObjectRef ABI. `MemberRef` adds member path and
uint64 extent without opening or decoding images. P2 local objects use
`repo_type=local` and `archive_format=tar`; remote revision retrieval is not implemented.

**Migration impact:** version 1 outputs must not be reused. Build a new output directory;
there is no in-place migration. Object IDs, record IDs, table columns, error codes,
summary counts, and crash hooks changed. Existing P1 query APIs remain unchanged but
are not automatically a query engine for the four domain tables. Historical P2 reports
describe the obsolete v1 protocol, not current certification.

## Four Arrow tables

- **objects:** stable `storage_id` profile, content-bound `object_id`, backend, nullable
  `repo_type`, `archive_format=tar`, dataset, object path/size/version, validator, and scan status. Local absolute build paths are never stored in logical identity.
- **samples:** retained RecordKey and record_id, source/post_id, image path/offset_data/size,
  nullable JSON path/offset_data/size, nullable text and physical image metadata, tags_state,
  tags:list<struct<value, category>>, hash_source and nullable sha256.
  Both image and JSON extents use uint64. No image decoder is imported.
- **annotations:** retained identity plus namespace/origin/tags_state and
  tags:list<struct<value, category>>; one grouped row per record/namespace/origin.
- **errors:** dataset_id, object_id, registry source, member/logical path, code, detail.
  Empty tables retain their exact schema.

Tags states: `known` is a nonempty list of strings, `empty` is `[]`, `missing` has no
configured metadata key, `invalid` has a wrong type. Missing/invalid tags are null.
Tag origin is `declared:json.<field>`; namespace/category come from the adapter.
Metadata SHA256 is only `declared:json.sha256`, never content proof. `--hash-images`
explicitly computes image SHA256 and marks `computed:sha256`. Default scanning does
not separately read images to hash them, **but the strong object validator reads the
whole TAR multiple times** (before scanning, before commit, and final input validation).
Size/mtime are change hints, not strong content proof. Validator version 1 explicitly
names strength `strong:sha256`.

## Registry and pairing

Source is mandatory registry data and never inferred from paths. Pairing preserves the
full logical stem. `image_prefix="images/"` and `metadata_prefix="meta/"` allow
`images/7.jpg` + `meta/7.json` to pair as `7`; remaining directory components are kept.
Prefix removal is explicit and collisions are rejected. `metadata_required` defaults
to true and can be false. `numeric_post_id` optionally requires decimal post stems.
`tags_field`, `text_field`, `max_json_bytes`, `tag_namespace`, `tag_category`, and
`image_extensions` are explicit options; defaults are `.jpg`, `.jpeg`, `.png`, `.webp`, and `.avif`. README.md/manifest.json, hidden paths,
directories, and unrecognized suffixes are ignored; `ignored_names` is configurable.

Error vocabulary: invalid_post_id, missing_image, missing_metadata, metadata_invalid,
source_mismatch, ambiguous_image_member, ambiguous_metadata_member, unsupported_member,
unsupported_archive, record_identity_conflict. Fatal archive/conflict/storage failures
raise and CLI returns error JSON/nonzero; per-record errors are stored and return 1.
Normal CLI returns 0; argument/fatal errors return 2.

## Publication, resume, and counts

Single writer, quiescent local inputs. Each shard writes four Parquet partial files in
batches, flushes/fsyncs and closes, then renames finals. COMMIT is last, atomically
published after fsync and records file path/size/SHA256/row counts, input validator,
schema/builder/dataset/object and created_at. POSIX fsyncs directories. Windows CRT
has no directory fsync: process recovery is tested, power-loss durability is not promised.
Readers must verify COMMIT before consuming files, never simply glob uncommitted finals.

Crash hooks: A=half samples partial before footer; B=all Parquet closed, no rename;
C=first final rename only; D=all final files without marker; E=marker published.
A–D rebuild; E validates all hashes/schemas/counts before skipping. Corrupt committed
outputs fail `CORRUPT_COMMIT` and are not silently rebuilt. Changed input content,
mtime, inventory, or configuration rejects existing offsets. Only reserved builder
partial names are removed; unrelated user files are preserved. Do not place your own
files in builder-reserved names. COMMIT hashes are not authenticated against an attacker.

Summary objects_seen/objects_committed/objects_skipped count TAR shards. `objects` is
the total shard rows including skipped shards; `samples` counts indexed physical sample
rows, not shards, tags, or invalid candidates. `skipped` is a compatibility alias of
objects_skipped. created_at and timings are nondeterministic; logical rows/IDs/counts
are deterministic. Memory still scales with the largest shard (headers and rows are
materialized); batched Parquet writing is not a bounded-memory streaming claim.

## Durable format compatibility

The current on-disk contract is `FORMAT_VERSION=4`, builder `sakurapool-p2-v4`.
`storage_id` is a stable configured profile identifier (default `local`), independent of
content-bound `object_id`. ObjectRef carries storage_id, object_id, object_path,
object_size, object_version, and validator. `archive_format=tar` is separate from
`repo_type=local`. Version 3 outputs are incompatible and must not be reused; build a
new output directory. Future ModelScope validation MUST NOT use remote full-object SHA
rereads as the normal validation path; use fixed revisions and object validators.
P4 MUST NOT use remote full-object SHA rereads as the normal ModelScope validation path.

## P3 runtime

The runtime compiles one or more committed P2 directories into an immutable
snapshot and answers queries from bitmaps without touching original archives.
The runtime format is 2, compiler `sakurapool-p3-v2`; v1 runtime snapshots are
rejected and must be recompiled. P2 durable FORMAT4 is unchanged, including
readability of partition build provenance recording runtime format 1. Snapshot
identity includes runtime format and compiler, so v1 and v2 do not collide.

```console
sakura runtime compile [--index DIR ...] [P2_DIR ...] RUNTIME_ROOT
sakura runtime query RUNTIME_ROOT [--source S] [--dataset D] [--namespace NS]
                 [--all-tag T] [--any-tag T] [--none-tag T] [--limit N] --full
sakura runtime query --snapshot RUNTIME_ROOT --spec SPEC.json
sakura runtime verify RUNTIME_ROOT [--full]
sakura runtime inspect RUNTIME_ROOT [--full]
sakura runtime lookup RUNTIME_ROOT --source SRC --post-id ID [--dataset DS]
```

`query` and `verify/inspect` also accept `--index`/`--snapshot` as aliases for
the positional root; `--spec SPEC.json` reads a spec object (including an
`any_of` list) and cannot be mixed with term flags. An unknown namespace is a
hard error even when no tags are given. `--full` runs the streaming full
verification (payload sha256 of every published file), while a plain open only
checks metadata and identity (below).

Layout per `RUNTIME_ROOT`: `current.json` (atomic pointer) plus
`snapshots/<snapshot_id>/{catalog.sqlite, bitmaps.sqlite, locations.npy,
SNAPSHOT.json, READY}`. Nothing is openable before `READY`; compilation is
stage-resumable (`STAGE.txt` marks stage1/catalog/bitmaps/locations/snapshot/ready).
A second `compile` reuses a published snapshot only after validating the READY
marker, the manifest, and a streamed sha256 of every published file; a broken
`current.json` is repaired, foreign `.staging-<other-id>` directories are never
deleted, and the P2 inventory fails closed on any undeclared file (stray
partials included), on `object_id != rel@sha256(input)`, and on fragment rows
that claim a different (dataset, object). `rid` is a dense `uint32` in
canonical order `(dataset_id, object_id, sample_path, record_id)`.
`locations.npy` is a rid-ordered numpy memmap (`object_idx u4, image_offset u8,
image_size u8, metadata_offset u8, metadata_size u8, format_id u2, flags u1`)
with an 82-byte identity trailer (`SAP3LOC1`, snapshot_id, rid_count) appended
after the payload: a fast open binds the whole file to the snapshot identity
(a same-shape `locations.npy` from another snapshot raises `SnapshotMixError`,
as does a whole-file swap of `bitmaps.sqlite`/`catalog.sqlite`), while an
in-place same-size payload edit is only detected by `--full` streaming
verification. `catalog.sqlite` holds names/IDs (records, sources, datasets,
namespaces, tags, tag_categories) and every lookup resolves via index
(asserted by `EXPLAIN QUERY PLAN` tests). Tag identity is `(namespace, value)`;
one namespace may be served by multiple sources/origins (bitmap union).
`tags_state in (known, empty)` defines a record's known set; `missing`/`invalid`
records contribute no tags. Categories are metadata: distinct non-null categories
are unioned into `tag_categories(tag_id, category)` (composite primary key,
declared FK to tags, index on `(category, tag_id)`). The compiler explicitly
validates references; it does not rely on SQLite FK enforcement. `tags` has no
category column. Staging `CATEGORIES.json` contains `[tag_id, category]` pairs
sorted by ID then category. `STAGE2-COUNTS.json` reports raw `tag_occurrences`;
formal `tag_memberships` sums final tag bitmap cardinalities, deduplicated across
categories, origins and chunks. `rt.tag_categories(namespace, value)` returns a
sorted tuple (empty for null-only categories); unknown tags/namespaces raise
`UnknownQueryValueError`. No category filter APIs are provided; queries remain
value-based. Offsets/sizes are stored lossless
beyond 2**63 and metadata presence is independent of its size (a zero-length
metadata keeps `HAS_METADATA`). The snapshot fingerprint excludes the P2
`created_at` timestamps by design, so re-timestamped identical inputs produce
the same `snapshot_id`.

Query domain (`RuntimeQuerySpec`): `sources` OR, `datasets` OR, then AND with
per-tag `all_tags`, one OR group of `any_tags`, and per-namespace `none_tags`
(`known_ns − excluded`); `any_of` is a union of flat branches. AND terms are
planned by their stored cardinality before any bitmap loads and materialization
stops once the intersection is empty. `query()` accepts a spec object or
keyword form (`rt.query(sources=..., namespace=..., all_tags=...)`) and returns
a lazy bitmap result: `count()` / `len()`, `iter_rids()`, `limit(n)`,
`iter_location_batches(size)`, `iter_record_batches(size)` (record batches carry
`source_name`/`dataset_name` alongside the numeric IDs); point resolution is
`lookup_rids(source, post_id[, dataset])` / `resolve_one(source, post_id,
[dataset])` (ambiguous matches raise `AmbiguousRecordError`) and
`object_ref(object_idx)` returns the full validated `ObjectRef`. Bitmaps are
cached in a byte-budget LRU (`cache_bytes` on `RuntimeSnapshot.open`, 256 MiB
default) with hits/misses/evictions counters; a single blob larger than the
budget is never cached.
