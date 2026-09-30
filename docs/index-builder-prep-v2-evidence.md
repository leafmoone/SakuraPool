# INDEX-BUILDER-PREP: runtime v2 candidate evidence

Previous candidate: `c986bf6f28c4dd831f4244ae0c44e27615c04d9b`.
Runtime implementation/canary SHA: `a5c141519503ddf6615f199c92a6e99085bf692d`.
Subsequent evidence/test-only commits do not change the installed runtime code.
Candidate branch only: `index-builder-prep-20260930`. External review required.

## Contract

P2 durable remains v4: per-record value + optional category is preserved in
samples/annotations. P3 v2 has TagKey `(namespace,value)`, one value bitmap,
and a sorted `tag_categories` relation for categories observed globally in the
snapshot. It does not describe category on an individual rid. Category-filter
queries are unsupported. Future per-record category bitmaps can be recompiled
from durable v4 without rescanning TARs. Old runtime v1 snapshots are rejected.
Compiler is `sakurapool-p3-v2`; format/compiler participate in snapshot identity.

`tag_occurrences` counts each P2 value/category occurrence; `tag_memberships`
is the sum of final value bitmap cardinalities (unique namespace,value,rid).
No category-specific bitmap is produced. Compiler validates category references;
no SQLite FK runtime enforcement claim is made.

## Validation

- Full Python suite: **588 passed, 3 skipped**, 169.41 seconds, including the
  large corpus tests and 1,000 randomized differential specs.
- Additional changed-content/same-path rejection: **4 multipart tests passed**.
- Ruff and diff checks pass.
- Rust `test --locked --all-targets`: all seven test binaries pass (58 tests).
- Rust `clippy --locked --all-targets -- -D warnings` and `build --locked --release` pass.
- Existing eight committed P2 canary roots revalidated and SHA-checked unchanged
  before/after v2 compilation. This is **P2 reuse, not another Rust scan**.
- All seven sources pass P3 v2 compile, full runtime verification, known-value
  query and local extent/image SHA verification.
- Danbooru two physical parts combine into a 232-rid v2 runtime. Adapter mismatch,
  duplicate object, and same-path changed-content inputs are rejected in tests.
- Fresh wheel installed into an independent target, executed with Python `-I`:
  authorized raw `danbooru/2024_nl/t149003-002.tar` -> Rust scan (not reuse) ->
  P2 -> P3 v2 -> tag query -> local extent SHA verification: PASS.

## Source-specific measured v2 canaries

One object per row; Danbooru intentionally has two. Category bytes below are
SQLite dbstat pages for relation + its two indexes, not exact incremental file
size. Bitmap bytes include serialized payloads of all existing bitmap kinds;
there is no new category bitmap. SQLite fixed overhead is material on tiny TARs.

| Source | Samples | Tag values | Tag/category pairs | Occurrences | Memberships | Category pages bytes | Bitmap payload bytes | Runtime total bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| anime_pictures | 635 | 2885 | 2885 | 16329 | 16329 | 122880 | 82676 | 903693 |
| bangumi | 12 | 114 | 114 | 208 | 208 | 8192 | 2360 | 125723 |
| danbooru part A | 1 | 30 | 30 | 30 | 30 | 8192 | 594 | 113002 |
| danbooru part B | 231 | 2183 | 2183 | 8417 | 8417 | 94208 | 53196 | 658559 |
| gamecg | 121 | 751 | 753 | 4184 | 4181 | 40960 | 21152 | 310204 |
| konachan | 321 | 1046 | 1046 | 3791 | 3791 | 49152 | 26292 | 412214 |
| yande | 23 | 113 | 113 | 188 | 188 | 8192 | 2370 | 126153 |
| zerochan | 1089 | 4776 | 4777 | 18813 | 18813 | 196608 | 120624 | 1400633 |

Actual conflict queries:
- `gamecg_native/sano_toshihide`: categories `(artist,general)`, rids `[69,70]`.
  Both value-query payloads verified. Same-rid multiple categories do not increase
  final value bitmap cardinality.
- `zerochan_native/tiger`: categories `(character,general)`, rids
  `[6,254,311,865,894]`. All five value-query payloads verified.

## Capacity estimates (bytes, LOW confidence)

These are estimates derived from representative canaries, not measured
full-repository index sizes. Method: source raw bytes times canary index/raw ratio.
Danbooru densities aggregate both observed objects. Tiny-object SQLite fixed
costs and nonrandom representative selection make extrapolations uncertain.
Known full sample counts and known-count estimates remain UNKNOWN. No scan was
performed to manufacture counts. These are v2 measurements, not v1 benchmarks.

| Source | Estimated durable | Estimated runtime | Measured runtime bytes/sample |
|---|---:|---:|---:|
| anime_pictures | 527252707 | 1116038031 | 1423.1 |
| bangumi | 10996784662 | 44086376214 | 10476.9 |
| danbooru | 5693558147 | 20331885057 | 3325.7 |
| gamecg | 3980264661 | 16624845408 | 2563.7 |
| konachan | 247294112 | 524551005 | 1284.2 |
| yande | 1595946697 | 5364314817 | 5484.9 |
| zerochan | 4046432220 | 7873172904 | 1286.2 |

Raw-density estimate totals: durable **27,087,533,206 bytes**, runtime
**95,921,183,436 bytes**. Known-count totals: UNKNOWN.

Runtime totals include catalog, bitmap and locations files plus manifests. Values
above 1 KiB/sample are explained by SQLite page/index overhead, tag vocabulary
and small sample denominators; no raw JSON/image payload storage was added.
No 1M/5M benchmark was run or claimed. P2 tags/captions remain existing durable
facts; the v2 change only adds observed category relation metadata to runtime.

## Local evidence locations (index machine)

Under `D:\WORK\instances\索引创建`:
- `v2-final-regression.log`, `v2-final-regression.xml`
- `v2-canary-a5c141519503/metrics.json`, `STATUS.json`, per-source runtimes
- `fresh-v2-a5c1415/PASS.json`, fresh durable/runtime outputs
- `v2-wheel/`, `installed-v2-wheel/`

Local evidence and TARs are not pushed into Git. The builder never deletes TARs.

## Review gate

P3_MULTI_CATEGORY_VALUE_MODEL = PASS
RUNTIME_FORMAT_V2 = PASS
P2_CATEGORY_FACTS_PRESERVED = PASS
VALUE_BITMAP_DEDUP = PASS
TAG_CATEGORIES_RELATION = PASS
CATEGORY_FILTER_QUERY = NOT_IMPLEMENTED_BY_DESIGN
REAL_GAMECG_CONFLICT = PASS
REAL_ZEROCHAN_CONFLICT = PASS
CANARY_7_SOURCES = PASS
MULTIPART_CANARY = PASS
FULL_REGRESSION = PASS
FRESH_WHEEL = PASS
INDEX_BUILDER_PREP = WAITING_REVIEW
FULL_INDEX_BUILD_STARTED = NO
FULL_INDEX_OBJECTS_SCANNED = 0
MERGED_TO_DEV = NO
DEV_MODIFIED_BY_THIS_MACHINE = NO
MAIN_MODIFIED = NO
R2_STARTED = NO
P5_STARTED = NO
