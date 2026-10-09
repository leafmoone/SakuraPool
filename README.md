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
Cargo.lock. The Windows build uses Rust 1.98.1 GNU and MSYS2 UCRT64.
For native Linux deployment, use the explicit host toolchain below. `rust/rust-toolchain.toml` pins
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

For offline indexing, runtime queries, publication verification and task
creation/inspection/export, install `.` without extras. Network operations require
`.[remote]` (Requests); the ModelScope SDK is not a runtime dependency. Its
`modelscope-hub 0.4.0` routes remain the reference for the bounded provider adapter.

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

### Native Linux deployment

On Debian 13 x86-64, use Python 3.12 or 3.13 with the pinned dependencies, a
native C compiler/linker, and Rust 1.98.1. Install prerequisites through your
system package manager and Rust's official distribution. From the repository
root in Bash:

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
```

`rust/rust-toolchain.toml` pins a Windows host, so retain the explicit native
Linux toolchain argument; do not rely on the directory override. On another
architecture, select that host's native triple and linker. The executable on
Linux is `rust/target/release/sakurapool-worker`, without `.exe`. The Python wheel
does not contain a prebuilt worker. Source distributions include Rust sources
and the lockfile. A populated toolchain/dependency cache allows adding `--offline`;
it does not replace a missing compiler, linker or dependency cache.

For restricted home directories, set `CARGO_HOME` and `RUSTUP_HOME` to writable
locations before installing/building. No system-level service is required for
the command-line workflow.

### Proxy and certificate configuration

Production requests honor standard `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and
`NO_PROXY` environment configuration, including lower-case forms supported by
Requests/reqwest. Python keeps automatic `.netrc` credential discovery disabled
and resolves proxies separately for each request URL. A separate credential-free
CDN client never receives the origin bearer token or session cookie. The Rust worker reuses its
bounded CDN connection pool within a lane (one idle connection per host,
15-second idle timeout); scheme changes or more than eight distinct CDN
authorities clear that pool. Redirect checks and disabled
automatic redirect following remain in place; local synthetic loopback transport
stays local even when proxies are configured.

Native metadata uses Python's effective per-URL proxy selection, including
`NO_PROXY`, then disables automatic proxy discovery in its dedicated Rust client.
It uses native system roots by default (`rustls-tls-native-roots`, including
`SSL_CERT_FILE`). An explicit `REQUESTS_CA_BUNDLE`, or otherwise `CURL_CA_BUNDLE`,
selects a PEM file of at most 2 MiB that replaces native roots for metadata;
CA directories, invalid bundles and unsupported proxy schemes fail explicitly.
The Rust image-transfer client continues to use native roots. Explicit legacy
Requests metadata retains its system CA file/directory fallback. Keep these
clients' trust settings consistent. HTTPS verification remains enabled. As with other applications
using system trust, a trusted TLS-inspecting proxy can inspect traffic; never
put tokens, cookies or signed URLs in logs.

### Exact metadata lookup bounds

Image retrieval uses 1,000-entry pages for exact pinned-object metadata lookup.
The provider response limit remains 1 MiB; no lookup walks more than 10,000 raw
entries, with the original ceiling of fifty logical tree-page reads applied
independently. Smaller provider pages can continue within those same bounds.
Repository identity, root scope, revision, size, SHA, duplicate-path and pagination
checks still apply. Discovery/admin defaults remain 200 entries per page.
A provider that truncates or silently clamps pages may reach the bounded walk
limit; that is an incomplete lookup, never evidence that the object is absent.
Oversized metadata responses fail rather than increasing the byte limit.

Production metadata uses a separate native worker advertising `metadata_attempt_v1`.
Each `read_metadata` call has one 125-second monotonic budget across ownership
and ledger waits, worker startup/rotation, handshake, pipe send/receive, up to
three HTTP attempts, response validation and retry waits. Each HTTP attempt has a whole-request deadline of at
most 60 seconds and a connection timeout of at most 10 seconds, clamped by the
time left. Production cancellation signals both data and metadata children
before bounded joins; they share one 5-second teardown grace. OS process creation and filesystem primitives are
not hard-real-time guarantees. This is a per-read bound, not a 125-second bound
for a repository lookup that can include an identity read and fifty page reads.

Successful metadata bodies stay in memory and anonymous IPC, using strict base64
inside a separately validated 2 MiB response envelope; ordinary RPC lines remain
at their configured capacity. Metadata never creates body sidecars. Header-only
decisions read zero application body bytes. Partial bodies or missing/invalid
worker acknowledgements are not replayed. Each control instance owns a separate
serial worker with its own 256-request rotation; it does not change image-worker
generations or conditional proofs. Accounted administration reserves the modeled
worker/response memory. Lightweight downloads keep the same fixed buffer bounds
and add one process per active control, without aggregate RAM admission.

Online `sakura remote inspect` requires `--worker /absolute/path/to/sakurapool-worker`.
The explicit `--legacy-requests` compatibility option (API:
`metadata_mode="legacy_requests"`) retains the former 10-second connect and
60-second socket-inactivity timeouts, without an absolute deadline. Missing or
old native capability never automatically selects it. Task startup checks the
metadata capability and limits before reconciliation and new item claims. Local
creation, inspection, queries and export, plus explicit offline fixtures, remain
usable without Rust. Byte and pagination bounds apply in every mode.

### Downloading on Linux

Install and verify the complete publication for the desired source using the
instructions below. Store the absolute native worker path in the private task
profile. Credentials must reference an authorized environment/file; do not put
the token itself in the profile or repository.

For images larger than the default 8 MiB per-image capacity, create a workspace
with a suitable technical capacity before creating the task. The example below
allows images up to 32 MiB while leaving individual network chunks at 8 MiB;
choose a larger image capacity when the selected images require it.

```bash
printf '%s\n' '{"capacity":{"image_max_bytes":33554432}}' > capacity.json
sakura workspace init WORKSPACE --config capacity.json
printf '%s\n' '{"sources":["danbooru"]}' > source-query.json
sakura publication verify local-index/sources/danbooru --full
sakura task create --workspace WORKSPACE \
  --publication local-index/sources/danbooru --query source-query.json \
  --selection first --limit 100 --task-dir WORKSPACE/tasks/images
sakura task run WORKSPACE/tasks/images --profile TASK_PROFILE.json --workers 6
sakura task inspect WORKSPACE/tasks/images
sakura task export WORKSPACE/tasks/images --manifest WORKSPACE/tasks/images/manifest.jsonl
```

Worker count is explicit; images in the same TAR can serialize despite multiple
workers. Existing tasks freeze their selection and execution capacity. Use the
supported resume workflow for interrupted tasks; do not replace their database,
reuse a task directory for a new selection or overwrite delivered files.


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
Large images may stream over several configured chunks. Each production transfer
RPC has a 125-second Rust deadline with request/body timeouts capped at 30 seconds
and clamped to the time remaining; Python uses a 130-second response watchdog
(the handshake retains 60 seconds). Blocking filesystem calls and pipe writes
are not guaranteed to terminate at an exact wall-clock instant.
The deadline includes the bounded Origin retries and CDN hop. Whole-object scans
use the same bound, so large-object completion is not guaranteed.
No change implies image decoding or conversion.

When archive-member metadata output is requested, a complete image and its
nonempty JSON member can share one data Range automatically. Their indexed
extents must be disjoint, in either physical order, and the entire span must fit
the configured Range chunk capacity (8 MiB by default). The gap must be at most
64 KiB and no more than one sixteenth of the combined member bytes. Otherwise
the existing separate, chunked reads apply; a failed combined request never
falls back to separate reads. Image-only requests are unchanged.

The combined request also reads the bounded gap bytes, so its full span counts
toward per-request buffers and transport checks. Only the exact image and JSON
slices are saved. Image bytes must match the publication SHA before writing;
JSON retains bounded validation and a receipt digest, without a publication JSON
SHA claim. Both files are synchronized before the unchanged PREPARED/publication
boundary. Next-request scheduling and generation hints use the full physical
span. This can reduce metadata-enabled data requests, but does not promise a
particular throughput gain or alter provider-control metadata RPCs.

Resume never reselects records. A published receipt must match task/operation,
file/directory identities and indexed image SHA before `DONE/VERIFIED`; valid
published output is reused, not redownloaded. Crashed `IN_PROGRESS` can become
`READY` only after claimed-before-network or exact owned-temp cleanup evidence.
A caught metadata/proof failure before image-stage creation carries explicit
pre-stage provenance; only a classified transient with safe control finalization
can use the bounded explicit-resume path. A missing stage alone is not evidence:
older unmarked failures and crashes before the stage stamp remain blocked.
Unknown/replaced output and ambiguous protocol failures remain `BLOCKED`, not
blindly retried. Classified transient, unpublished failures require explicit
resume; at most two recovery retries survive process restarts. This is not an
end-to-end exactly-once or Windows directory power-loss guarantee.

A hot rollback journal from an interrupted v4 task is validated using a bounded
private recovered snapshot before the original task can be opened for writing.
Read-only inspection/export uses that snapshot and leaves the original database
and journal unchanged; an authorized writable open holds the runner lock and
lets SQLite recover the original. Invalid, oversized or changing inputs fail
closed. Recovery does not turn an uncertain delivery into a verified receipt.
Automatic hot-journal recovery requires Python 3.11+ for exact SQLite error
codes. Python 3.10 remains supported for ordinary operations, but this recovery
case reports `TASKDB_RECOVERY_UNSUPPORTED` without writing the original. Reopen
the same task using Python 3.11+; do not delete its journal. Hot legacy archives
remain blocked rather than automatically migrated.

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

For immediate creation and download, `task start` accepts the same creation options
plus `--profile`, `--workers` and `--workers-per-tar`:

```text
sakura task start --publication PUB --query QUERY.json --selection first --limit 100 --metadata --task-dir TASK --profile PROFILE.json --workers 8 --workers-per-tar 8
```

On POSIX this uninterrupted operation fully verifies content once and owns the same
open publication through task creation and execution. Inputs must remain immutable;
file/handle identity and change signatures are checked before the runner connects.
Keep the task directory outside the publication. There is no persisted verification
token: standalone `create`, `run` and `resume` retain their full-verification behavior.
Windows currently keeps both full verifications in `start` as well: its stat creation
time cannot serve as a write/change guard. The other download optimizations apply
independently of this startup fallback.
The Python equivalent is `tasks.runner.create_and_run_task`.

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

The coordinator may commit up to 32 already queued independent events together;
each lane waits for the FULL/DELETE commit before advancing. It never waits to fill
a batch. READY lookahead and prepared TAR heads stay bounded to 16 times the active
lane limit. Native lanes may reuse bounded unbound metadata candidates from the
same run and fixed revision, but every cold lane/object binding still performs its
own conditional proof. Metadata controls are created on misses, so all-distinct TAR
workloads can still require a control per lane; there is no universal process cap.

There may be a visible image before its JSON. Direct/session calls without TaskDB
preserve partial data on failure and do not promise automatic restart/adoption.
Unsupported no-replace platforms fail closed; there is no fallback to overwriting
rename and no Windows power-loss/exactly-once promise. No consumption ledger is added.

## Raw image delivery settings

Publication/task downloads support registered `.jpg`, `.jpeg`, `.png`, `.webp`,
`.avif`, and `.gif` formats. New tasks and default adapter configurations enable
all six, including GIF. Existing tasks retain their saved format settings; older
tasks without saved settings retain the historical first-five policy.
`publication fetch` and `task create` accept `--image-extensions` as comma-separated
suffixes. Bytes are verified and delivered unchanged; no decoding or conversion
is performed. Unknown formats and unsafe filenames are rejected. Index adapter
`image_extensions` controls scanning and does not automatically enable download
formats. Explicitly restricted task settings are checked against the selected
records before task creation or network startup. Rejections report bounded format
counts and record identities rather than treating a disabled format as corruption;
no records are silently filtered or converted.

New v4 tasks can explicitly expand their format selection without reselecting records:

```text
sakura task inspect /workspace/tasks/task
sakura task update /workspace/tasks/task --expected-settings-version 0 --image-extensions .jpg,.jpeg,.png,.webp,.avif,.gif
```

Use the actual `settings_version` from inspect. New tasks start at version zero
with all six defaults. Updates use compare-and-swap, reject format
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
Task `run` and `resume` also accept `--workers-per-tar` (API: `workers_per_tar`),
defaulting to 1. Increase it explicitly to allow bounded concurrent operations on
the same TAR, for example:
`sakura task run TASK --profile PROFILE.json --workers 6 --workers-per-tar 2`.
Both options apply to the current run, not the frozen task plan.
Actual concurrency depends on ready work, object distribution and available OS
resources. Earlier P3/P4 CLI examples below are historical/admin interfaces,
not the default task workflow.

Task `run` and `resume` retain one fully verified publication handle under the
runner lock. Task identity, profile allowlist and physical output root are checked
before resolving credentials or starting an owned transport. The same verified
handle is used for reconciliation and downloads, then closed when the run ends.
Caller-supplied API transports remain caller-owned and still require full
publication verification for each run.

### Speed and concurrency limits

The previous 1/2/4/6 choices and estimated 512 MiB aggregate-memory admission are
removed by explicit user contract change. Zero, negative and non-integer workers
(including API booleans) are invalid; default remains 1. No machine-RAM detection,
automatic worker increase or replacement memory ceiling is introduced. Choosing
large concurrency can exhaust real machine resources; successful execution or
throughput growth is not guaranteed.
The same strict positive-integer validation applies to `workers_per_tar`.

Let L be the smaller of requested workers and the ready-record count at runner start. Lanes are
created lazily for eligible work, not eagerly for the full input. Active
operations are bounded by L, candidate/claim lookahead by 16L, event envelopes
by 2L, and completion envelopes by L. Immutable prepared descriptors are cached
across scheduling passes within one run, trimmed to the current lookahead window,
and evicted on claim. They never carry or share lane-specific network proofs.
Claims recheck exact typed record identity and bounded READY-window rank in their
write transaction without copying the entire candidate window again. When that window
contains only busy TARs, scanning waits for a completion; pause/cancel polling
and event acknowledgements continue. The SQL LIMIT binding is capped at SQLite's signed 64-bit maximum; the READY
filter naturally limits returned rows. Very large positive Python integers therefore
do not overflow SQLite or impose a hidden input maximum. There is no fixed 12-record candidate ceiling.
Each full TAR transport identity (endpoint, repository, repository type, revision,
path and size) has at most min(workers_per_tar, L) active operations. The default
retains one active operation per TAR. Opting in can help when few TARs and slow
responses leave workers idle, but adds independent worker startup, metadata and
conditional-proof work; fast responses may be slower. Native lanes retain their
own transport, metadata channel, generation and bounded proof caches. No lane
borrows another lane's authorization. With `workers_per_tar` above 1, a custom
transport's `clone()` must return a fresh instance, never the caller's transport
or another lane, even when the effective cap is 1. With `workers=1`, a transport
without `clone()` can still be borrowed for the single lane and remains caller-owned.
For opt-in same-TAR concurrency, each dispatch first admits visible TARs with no
active operation. Only when none exist in the bounded lookahead may it add an
operation to an already-active TAR, up to the per-TAR cap. This spreads available
lanes across independent TARs before filling spare lanes with same-TAR work.
Warm affinity cannot move a record from the second admission layer ahead of the
first. An already-active TAR continues making progress; when its last active
operation finishes, its next visible READY record can enter the first layer. No operation
is preempted, and independent TARs beyond 16L are not searched for.
In this opt-in mode, the fairness allowance below applies to the currently
eligible admission layer; it does not promise a second lane while independent
TAR work is visible. The default cap of 1 retains its original scheduling rule.
Within the bounded lookahead and eligible layer,
the oldest eligible record is preferred when a free lane has its live binding.
Otherwise a later eligible record with a matching warm free lane may run first,
but at most L consecutive successful dispatches may bypass the oldest eligible
record. The next dispatch then serves that oldest record, even on a cold lane;
serving the oldest resets the allowance. Blocked scans and rejected claims do not
change it. Without a usable warm candidate, eligible-record and free-lane FIFO
order are used. Frozen selection, sequence numbers and output names never change.
These next-chunk predictions are scheduling hints, not authorization: data-plane
binding and conditional proofs are still validated. Per-request chunk/header/RPC,
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
Sampling reuses the canonical seed-hash prefix while preserving the versioned
hash input, rank tie-breaks, complete candidate validation and heap limits. The
same publication, query, seed and selection settings produce the same frozen
record order and selection/plan digests.

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
and bounded sidecar, spool and phase-specific memory/disk limits, and
[local partition builder](docs/local-partition-builder.md)
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
rather than a retired virtual-environment path. Development tests and validation
working artifacts are excluded from version control; product branches and release
distributions intentionally exclude tests, reports and plans.

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
`image_extensions` are explicit options; defaults are `.jpg`, `.jpeg`, `.png`, `.webp`,
`.avif`, and `.gif`. Saved explicit lists remain unchanged. README.md/manifest.json, hidden paths,
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
Inventory ownership checks validate projected Arrow batches without materializing
every ordinary string row in Python; unsupported or invalid batches retain the
row-level validation path. Catalog compilation checks object metadata one batch
at a time, accepting identical duplicates and rejecting conflicting rows without
retaining the whole fragment. These paths preserve input diagnostics, canonical
ordering and published snapshot bytes.
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

To select images whose width **and** height are strictly greater than 1024 pixels,
use `{"width_gt":1024,"height_gt":1024}` in the query JSON (`--spec` for runtime
queries, `--query` for task creation), or
`rt.query(width_gt=1024, height_gt=1024)`. Each optional threshold is a nonnegative
integer below 2**32. Null/zero dimensions do not match; exactly 1024 does not match.
These conditions AND with source/dataset/tag conditions. For `any_of`, put them
inside each relevant branch. Dimensions are read from the local P2 index, without
fetching images. Recompile existing runtimes from their P2 inputs before using
these filters; old snapshots still support unfiltered queries, and a dimension
query on an old snapshot reports that recompilation is required. For task creation,
rebuild the index publication from the new runtime as well.

Size bounds also accept `width_lt`, `height_lt`, `pixels_gt`, `pixels_lt`,
`aspect_ratio_gt`, and `aspect_ratio_lt`. All bounds are strict (`gt` means `>`,
`lt` means `<`) and AND together. Pixels mean `width * height`; aspect ratio means
`width / height`. Example: `{"width_gt":1024,"pixels_lt":4000000,
"aspect_ratio_gt":"16/9","aspect_ratio_lt":2}` selects wide images below four
million pixels. Use `"16/9"` for an exact fractional ratio; finite numeric inputs
use their decimal spelling (so `1.5` means exactly `3/2`). Ratio input strings and canonical fractions
are limited to 1024 characters; scientific notation in strings is not accepted.
Pixel thresholds are nonnegative integers below 2**64; ratio thresholds are
nonnegative. Null or zero in a tested dimension never matches, including upper
bounds; pixels/ratio require both dimensions to be positive. Contradictory bounds
produce no matches. Only width/height are stored: pixels and ratios are computed
with exact integer arithmetic from streamed/batched candidates, without SQLite
multiplication overflow, floating-point boundary rounding, or image fetches.
These bounds work in query JSON, `RuntimeQuerySpec`, and `rt.query(...)`; task
freezing retains them and canonicalizes equivalent ratio spellings.

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
