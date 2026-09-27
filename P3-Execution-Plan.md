# SakuraPool P3 — Runtime Snapshot / Bitmap Query Engine

以下为本轮用户执行单的逐节执行条款转录（保留全部 0–75 节的要求；排版合并，不以代理摘要替代门禁）。用户当前明确指令仍为最高任务依据。

## 0. 阶段状态
P2 已完成、审查、fast-forward 合并并推送。BASE_SHA、origin/main、origin/dev 均为 `6c45548e01b0856d6d4bbce18e240be0b22ae7e7`。P3 所有开发在 dev，每个持久提交正常 `git push origin dev`，并 `git ls-remote origin refs/heads/dev` 核验远端 SHA。禁止修改 main。
阶段结束：implementation commit → push dev → 完整验证 → report/evidence commit → push dev → WAITING_REVIEW。外部 reviewer 直接通过 GitHub 审查 commit。不得自行 merge main，不得开始 P4。

## 1. P3 唯一目标
P2 committed Parquet fragments → runtime compile → immutable snapshot → Roaring bitmaps / mmap locations / SQLite catalog → query engine → QueryResult / LocationBatch。
必须支持：
```python
snapshot = RuntimeSnapshot.open(...)
result = snapshot.query(
    sources=["danbooru", "zerochan"], namespace="danbooru",
    all_tags=["1girl", "solo"], any_tags=["blue_hair", "pink_hair"],
    none_tags=["monochrome"],
)
result.count()
for batch in result.iter_location_batches():
    ...
```
结果为匹配记录及其 TAR byte extents；不实际下载图片。

## 2. 明确禁止范围
不实现 ModelScope HTTP、HTTP Range、token/auth、下载器、RetrievalPlanner 网络执行、整 shard 下载、去重、PDQ/pHash、GUI、生产 19TB 扫描、真实 ModelScope 数据访问。只处理本地 P2 index → runtime snapshot → query/location。

## 3. 新增依赖
固定测试 `pyarrow==18.1.0`、`numpy==2.2.6`、`pyroaring==1.1.0`。NumPy 2.2.6 保持 Python 3.10–3.13 支持；PyRoaring 1.1.0 用于 32-bit Roaring runtime。不得提高 Python 下限到 3.12；`requires-python >=3.10,<3.14` 保持。Python 3.12 为 pinned certification。若 uv 可直接取得 Python 3.10、3.13，增加 wheel/install/import smoke，不要求二者全跑性能 benchmark。

## 4. Runtime 版本与 P2 分离
P2 durable `FORMAT_VERSION=4` 不复用。建立 `RUNTIME_FORMAT_VERSION=1`、`RUNTIME_COMPILER="sakurapool-p3-v1"`，两个独立版本域。

## 5. 输入只接受 committed P2 fragments
Compiler 输入 P2 index directory；仅消费 INPUT.json、*.COMMIT、COMMIT 声明的 objects/samples/annotations/errors parquet。不得 glob 所有 parquet 后信任。编译前验证 COMMIT 存在、schema=4、builder 兼容、fragment 路径/大小/SHA256/Arrow schema/row count 正确、无额外未知 COMMIT。partial、缺文件、损坏、schema 不匹配、混合 P2 版本、COMMIT hash 错误全部 fail closed；不自动修复 durable index。

## 6. Runtime Snapshot 目录
建议：
```
runtime/current.json
runtime/snapshots/<snapshot_id>/SNAPSHOT.json
runtime/snapshots/<snapshot_id>/catalog.sqlite
runtime/snapshots/<snapshot_id>/bitmaps.sqlite
runtime/snapshots/<snapshot_id>/locations/object_idx.npy
runtime/snapshots/<snapshot_id>/locations/image_offset.npy
runtime/snapshots/<snapshot_id>/locations/image_size.npy
runtime/snapshots/<snapshot_id>/locations/metadata_offset.npy
runtime/snapshots/<snapshot_id>/locations/metadata_size.npy
runtime/snapshots/<snapshot_id>/locations/format_id.npy
runtime/snapshots/<snapshot_id>/locations/flags.npy
runtime/snapshots/<snapshot_id>/READY
```
最终 snapshot immutable，runtime 不得修改其中任何文件。

## 7. snapshot_id
source_fingerprint 来自 canonical sorted P2 committed inventory，例如 dataset_id、object_id、input validator、contract_sha256、四 fragment sha256。created_at、绝对本地路径、worker 顺序、临时目录名不得进入语义 fingerprint。
`snapshot_id = SHA256(runtime_format_version + compiler version + source_fingerprint + compile options)`。完整输入相同，snapshot_id、rid 映射、tag_id 映射、查询结果相同。

## 8. rid
dense uint32，合法 0..2^32-1；sample_count>2^32 必须拒绝。约 21M 足够。rid 仅 snapshot 内有效；所有 QueryResult 绑定 snapshot_id，不把裸 rid 当长期身份。

## 9. rid canonical 顺序
按 dataset_id、object_id、sample_path、record_id 升序分配，不按 worker/filesystem/并发顺序。同一 TAR 样本基本连续，升序 rid 具备 shard locality。

## 10. Compiler 内存
不得把 21M samples 或完整 corpus 的 annotations/memberships 放 Python list。允许临时 SQLite staging、Arrow batches、mmap；约 21M 行的一行/sample compiler-staging.sqlite 可接受。禁止长期 420M (tag_id,rid) SQLite runtime 表。

## 11. catalog.sqlite
至少 meta、sources、datasets、objects、formats、records、tags、namespaces。
```
meta: snapshot_id,runtime_format_version,compiler,source_fingerprint,rid_count
sources: source_id,name UNIQUE
datasets: dataset_id,name UNIQUE,source_id
objects: object_idx,storage_id,object_id,object_path,object_size,object_version,
 validator,backend,repo_type,archive_format,validator_kind,validator_strength
formats: format_id,format
records: rid PRIMARY KEY,record_id UNIQUE,source_id,dataset_id,post_id
tags: tag_id,namespace_id,value,category,cardinality
namespaces: namespace_id,namespace
```
record_id 在 runtime SQLite 建议存 16-byte BLOB，API 输出恢复 hex。

## 12. Point lookup
至少 UNIQUE(record_id)、INDEX records_source_post(source_id,post_id)、INDEX records_dataset_post(dataset_id,post_id)。source/post 可返回 0/1/N rid，跨 dataset 同逻辑 post 不覆盖。提供 lookup_rids(source,post_id,dataset=None)、resolve_one；多匹配抛 AmbiguousRecordError。

## 13. mmap Location Index
Array index==rid；`.npy + mmap_mode="r"`。
```
object_idx uint32
image_offset uint64
image_size uint64
metadata_offset uint64
metadata_size uint64
format_id uint16
flags uint8
```
不使用 object/path 字符串数组。metadata 缺失：flags & HAS_METADATA==0，offset/size=0；flag 决定语义，不能仅 offset=0 代表缺失。

## 14. size uint64
不能退为 uint32，不能引入 4 GiB 限制。核心约 39 bytes/sample，21M 数量级约 0.8GB，npy header 可忽略；这是规划数量级非实测。

## 15. ObjectRef
object_idx → catalog objects → 完整 ObjectRef：storage_id/object_id/object_path/object_size/object_version/validator/backend/repo_type/archive_format/validator_kind/validator_strength。P4 不得回 durable Parquet 补字段。

## 16. Tag identity
TagKey=(namespace,value)，不是裸 string，也不是(source,tag)。source=zerochan、namespace=danbooru、tag=1girl 合法；图片来源与词表分离。

## 17. category 冲突
同(namespace,value)：0个非null category→NULL；1个唯一非null→该值；>1不同非null→TAG_CATEGORY_CONFLICT compile失败，禁止最后覆盖。

## 18. annotation→Bitmap
P2 record_id/namespace/origin/tags_state/tags。runtime (namespace,tag)→BitMap[rid]。同namespace多个origin默认union；P3无origin-specific查询，origin仍保留durable，未来可增加runtime索引而不改tag identity。

## 19. tags_state与NOT
known/empty 是该namespace已知；missing/invalid未知。生成namespace_known_bitmap。NOT monochrome必须scope AND namespace_known MINUS monochrome，禁止universe-minus把未知当确认无标签。

## 20. Roaring类型
使用pyroaring.BitMap 32-bit，不使用BitMap64。

## 21. Bitmap存储
bitmaps.sqlite每逻辑集合一行，例如tag_bitmaps/source_bitmaps/dataset_bitmaps/namespace_known_bitmaps。每行id/cardinality/serialized_bytes/blob_sha256/bitmap BLOB；禁止每membership一行在线SQLite表。

## 22. Bitmap分块构建
不能整个corpus全部tag bitmap常驻。chunked partial build：约250k～1M annotation records一chunk，构建当前tag_id→BitMap，serialize写bitmap_parts.sqlite后释放。parts含tag_id/part_no/cardinality/blob。完成后逐tag读parts OR merge，写最终bitmaps.sqlite后释放。内存只关联一个chunk的tag集合+单个最终tag bitmap，不是全corpus所有tag bitmap。

## 23. tag_id
snapshot-local uint32，deterministic。处理顺序canonical fragment、record/rid、namespace、origin、tags sorted(value,category)，可first-seen分配；或最终lexical排序，但不能引发420M memberships大型remap。双编译tag_id一致，优先简单可验证。

## 24. Source/Dataset bitmap
source→BitMap[rid]，dataset→BitMap[rid]；数量少可内存构建，同样写bitmaps.sqlite。

## 25. Bitmap Cache
byte-budget LRU默认256MiB，不能仅限制100tag等数量。可用serialized blob bytes近似cost。报告hit/miss/eviction/bytes。

## 26. Query Spec
建立独立domain RuntimeQuerySpec(sources=[],datasets=[],namespace=None,all_tags=[],any_tags=[],none_tags=[])，支持any_of多branch，不破坏P1 generic QuerySpec。

## 27. Query语义
单branch sources OR，datasets OR，两scope AND；all AND，any OR，none在known namespace中排除，tag filters与scope AND；any_of branches OR。

## 28. 未知条件
unknown source/dataset/namespace/tag均显式错误，不静默empty，避免拼错标签假装没有结果。

## 29. Bitmap Planner
AND根据stored cardinality先执行最小集合，不按用户输入机械排序；OR正常union。

## 30. QueryResult
惰性不是list[SampleLocation]，支持snapshot_id/count()/len()/limit(n)/iter_rids()/iter_location_batches(batch_size=8192)。可有iter_records但必须batch；不得1M结果生成1M dataclass。

## 31. lazy limit
limit(1000)不得先materialize 2M rids再切；bitmap iterator+islice或等价。

## 32. LocationBatch
snapshot_id、rids[]、object_idx[]、image_offset[]、image_size[]、metadata_offset[]、metadata_size[]、format_id[]、flags[]；可NumPy数组，只对当前batch advanced indexing copy，内存与batch_size相关。

## 33. Identity Batch
提供iter_record_batches，以SQLite batch query取source/post_id/dataset/record_id，不每rid一次SELECT。

## 34. immutable发布
.staging-<id>/编译。catalog/bitmaps完成close、locations flush/close→逐文件验证→SNAPSHOT.json→READY→atomic rename snapshots/<id>→atomic current.json。READY之前不可open。

## 35. SQLite发布状态
无遗留WAL；close并确认-wal/-shm不存在。runtime readonly，不写usage/LRU/last_access；这些只在进程内存。

## 36. SNAPSHOT.json
至少snapshot_id/runtime_format_version/compiler/source_fingerprint/rid_count/object_count/source_count/dataset_count/tag_count/tag_memberships，python/pyarrow/numpy/pyroaring依赖版本，locations dtype/shape，每文件path/bytes/sha256，created_at。created_at不参与snapshot_id。

## 37. open验证
默认不能每次hash1GB npy。快速验证READY/SNAPSHOT解析/文件存在与size/SQLite meta.snapshot_id/npy dtype shape/rid_count。另CLI runtime verify --full验证完整SHA。

## 38. 混snapshot
A locations+B SQLite必须拒绝，不能产生看似合法但位置错误的数据。

## 39. CLI
```
sakura runtime compile --index <P2-index> --output <runtime-root>
sakura runtime query --snapshot <root-or-snapshot> --spec query.json
sakura runtime inspect --snapshot ...
sakura runtime verify --snapshot ... --full
```
query例如输出snapshot_id/count/limit/rids。不得增加download命令。

## 40. Point CLI
可增加runtime lookup --snapshot ... --source danbooru --post-id 123456；多版本返回多条，--one歧义非零失败。

## 41. Reference
保留小数据Python set reference evaluator，bitmap结果逐rid完全一致，不能只比较cardinality。

## 42. Random differential
程序生成>=1000随机query specs，包含source/dataset/all/any/none/mixed/multi-source/any_of；reference set与Roaring逐rid一致。

## 43. NOT专项
A known monochrome；B known无monochrome；C empty；D missing；E invalid。NOT monochrome仅B/C，不含D/E。

## 44. 多source共namespace
source A:1girl/solo；source B:1girl/blue_hair；namespace danbooru，sources A OR B AND1girl两者匹配，证明namespace!=source。

## 45. 多dataset同source/post
danbooru/123/dataset_old与dataset_new两个rid共存，lookup(source,post_id)两条，加dataset_new一条。

## 46. Bitmap序列化
serialize→SQLiteBLOB→freshprocess deserialize，cardinality/元素一致；blob hash mismatch拒绝。

## 47. mmap freshprocess
Compiler退出后全新Python进程打开，验证mmap/SQLite/bitmap/query/locationbatch，不依赖残留对象。

## 48. Compiler crash/resume
注入A catalog完成前/B catalog完成/C locations部分完成/D bitmap完成/E SNAPSHOT写完READY前/F READY完成。A-E不可open、重试重建；F验证后直接复用。

## 49. current.json
仅完整READY后更新；current更新失败旧current保持有效，新snapshot可显式id打开；不指向半成品。

## 50. 临时资源
stagingSQLite/bitmap_parts/partialnpy/临时snapshot目录明确ownership、异常清理，不删用户文件。可借鉴P2scope+handle，不无必要复制大量代码。

## 51. Benchmark输入
无生产访问。直接生成符合P2最终schema/COMMIT的synthetic Parquet，不必造真实图片。至少100k correctness corpus、1M performance、5M scaling。

## 52. Tag分布
不能均匀随机；hot~70%、medium~10%、rare~0.1%、相关/互斥tag、多source、多dataset、missing/invalid。报告平均memberships/sample。

## 53. Query benchmark families
source only、hot、rare、hot AND medium、3tag AND、3tag OR、NOT、source+2tag、2source+2tag、any_of2branches。每类多次运行。

## 54. 冷热
process-cold=新RuntimeSnapshot/空LRU；warm=bitmap已缓存；process-cold不是physical disk cold，OS cache可能已热。

## 55. 性能目标
5M synthetic warm典型count p95<=200ms；query到首128locations p95<=500ms；10k位置<=1s。未达不降规模，报告PERFORMANCE_GATE_FAIL并profiling；轻微偏差由reviewer判断阻塞。

## 56. 大结果
>1M matching rids count()+limit(100)，证明没生成1M Python SampleLocation对象，报告peakRSS。

## 57. Compile Memory Gate
5M证明无全records Pythonlist、全memberships Pythonlist、全tagfinal bitmap常驻。记录peakRSS/peak tempdisk/final snapshotbytes；比较1M vs5M增长而非任意绝对数宣称bounded。

## 58. Runtime size
catalog bytes/bitmaps bytes/locations bytes/bytes per sample/bytes per tag membership/bitmap compression ratio；为21M规划，不冒充真实库数据。

## 59. Cache benchmark
测miss/hit/eviction，默认256MiB并允许32MiB，测真实eviction；不可设置32MiB却增长GB。

## 60. 5M不是21M保证
明确synthetic，不声称21M必然xxms；真实21M留P6或真实索引后。

## 61. runtime一致性
QueryResult持snapshot handle/id；current换新snapshot，旧result仍只解析原snapshot，须测试。

## 62. 生命周期
支持with RuntimeSnapshot.open(path)。关闭后访问应明确SnapshotClosedError或文档规定result持有生命周期，不能不确定。

## 63. 模块建议
src/sakurapool/runtime/{compiler,snapshot,catalog,bitmap,locations,query,errors}.py，可适配现有结构，不为目录美观重构P2 indexer。

## 64. transient SQLite
compiler-staging.sqlite和bitmap_parts.sqlite不能留在最终snapshot；编译暂存不是runtimeAPI。

## 65. 不加入DuckDB核心
只Roaring在线集合、SQLitecatalog、mmap位置，不同时建DuckDB/SQLite taghits多套查询。DuckDB将来单独讨论ad-hoc fallback。

## 66. 核心测试矩阵
P2committed验证/deterministic rid/uint32边界/重复recordid失败/objectmetadata冲突/namespace隔离/category冲突/四tags_state/allanynone/multi-source/multi-dataset/同post多record/any_of/unknown错误/bitmapcorrupt/catalogcorrupt/mmapdtype shape corrupt/snapshotmix/partial/currentatomic/freshprocess/cacheeviction/lazymillion全部覆盖。

## 67. P1/P2回归
原102tests全部继续通过。如需P2durable变更STOP报告P2_CONTRACT_CHANGE_REQUIRED，不偷偷改已合并v4；正常P3不需变P2schema。

## 68. Dependency验证
Python3.12/PyArrow18.1/NumPy2.2.6/PyRoaring1.1正常依赖安装、pipcheck、wheelimport、fullpytest、ruff、CLIsmoke；不得--no-deps代替认证。

## 69. GitHub remote
每正式implementation及report commit均push origin dev，ls-remote核验LOCAL_DEV_SHA==REMOTE_DEV_SHA。最终报告origin/main/dev。

## 70. Git提交
可少量可审查implementation commits：compile/query/testscale等，不强行squash。报告BASE/IMPLEMENTATION_START/IMPLEMENTATION_END/SUBMISSION；END为最后代码/测试commit，之后只report/evidence。

## 71. Report
至少reports/P3/report.md、evidence-manifest.json、benchmark.json、query-benchmark.json。解释layout/rid排序/tagid规则/完整性/bitmap partialmerge/SQLschema/mmapdtype/lazy/cache/publish recovery。

## 72. Benchmark字段
records/unique_tags/tag_memberships/compile_seconds/peak_rss/peak_temp_disk/snapshot_bytes/catalog_bytes/bitmap_bytes/location_bytes/queryfamily/resultcardinality/processcoldlatency/warmp50 p95 p99/first128/10klocations/100klocations/cachehit miss eviction。

## 73. Evidence
每项command/cwd/exit/bytes/sha256。至少pytest/pinnedpytest/ruff/BASEdiff/wheelbuild/install/pipcheck/CLIsmoke/runtimecompile/fullverify/differential/1Mbenchmark/5Mbenchmark/recovery。

## 74. 最终用户交付
完整可复制正文列BASE/IMPLEMENTATION_START/END/SUBMISSION/main/dev/originmain/dev/runtimeversion/validation fixture snapshotid/pytest/pinnedpytest/ruff/diff/wheel/pipcheck/100kcorrectness/1Mbenchmark/5Mbenchmark/reportpath/evidencepath/NO ModelScope/NO生产/NO P4/NO main/WAITING_REVIEW。已远端化，不主动导完整diff，外部reviewer从https://github.com/leafmoone/SakuraPool读真实commit/blob。

## 75. P3后边界
审查通过具体dev提交→明确授权main ff→主分支复验push远端核SHA→P3 CLOSED；然后才P4 StorageBackend/ModelScopeBackend/HTTPRange/ObjectRef远端/MemberRefRange/206Content-Range/imageJSON mergedrange。本阶段不提前启动。
