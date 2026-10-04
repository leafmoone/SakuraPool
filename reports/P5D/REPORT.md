# P5-D 最小收敛与可用输入验证报告

## 状态、范围与身份

```text
P5_D = WAITING_REVIEW
BASE_SHA = a9c93f8283573915633c8f70099e7f88267f2ea6
IMPLEMENTATION_COMMIT = e7eb75016659ea411c6bec9fc35a8c2d70fc6697
IMPLEMENTATION_TREE = 1a4ddd59cbaf34c282c8abac8f05be55a39940ed
FINAL_COMMIT = 交付正文列实际提交（报告不自引用）
INPUT_COVERAGE = NOT_AVAILABLE（生产规模）；PRODUCTION_INPUT = NOT_AVAILABLE
CANARY = 121 records /1 source /1 object
P5_D_VALIDATION = PARTIAL
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0
INDEX_BUILD_SHA_CHANGED = NO
DOWNLOAD_ARCHIVE_SIZE = NOT_MEASURED
```

最小代码/文档收敛和批准小canary验证已完成；生产规模门未完成，不称全规模COMPLETE。尚缺管理员完成清单、明确已完成且冻结的P2/P3/Publication路径及source/partition/object/record覆盖和lineage；不能推算715片齐备。仅P2不自动编译全库，不登录索引机、不派扫描、不广搜未知目录。

## 最小改动与验证

image hash每batch仅一次to_pylist，保整批校验后executemany、4096上限、重复record拒绝、事务与错误；generator只在此调用当场消费。README/ROADMAP标C已合并、D验证中、默认workers1、显式2/4上限；区分串行同线程PublicationSession与task coordinator lanes；保8MiB member/legacy ledger/通用workspace迁移未实现。pause/cancel观察后停新claim，已准入项drain后返回。未改pipeline、格式、预算或Rust。

`pytest tests/test_publication.py -q --tb=short` 首轮退出1：55 passed /2 failed /52.01s，新增测试代理缺contextmanager，非产品失败。代理修后新2节点passed /.72s；再补真实SQLite插入观测后最终2 passed /.73s、退出0：valid转换1/insert1/SHA等价，后置非法row原错误且整批insert0/noREADY。相关Ruff/diff通过；55+2分轮，不冒57同轮全过。sole最小代码/测试PASS，随后仅README顺序措辞修、不重测。

keeper3.13.15、实际import当前p5a-clean/src；显式既有Fix3 release Rust worker。未变Rust/协议/依赖，不重build或C完整Python/310/wheel/stress矩阵。必要复现脚本measure_publication.py及real_canary.py，精简容量/API数据canary_metrics.json；没有重复日志或目录hash清单。

已有实际命令（在 `D:/SakuraTool/SakuraPool-p5a-clean` 执行，不为文档重跑）：
```bash
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe reports/P5D/measure_publication.py --smoke
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -m ruff check reports/P5D/measure_publication.py
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe reports/P5D/measure_publication.py --publication D:/SakuraTool/SakuraPool-R2C3-real-20261002/publication --label approved_canary > reports/P5D/canary_metrics.json
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -m ruff check reports/P5D/real_canary.py
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe reports/P5D/real_canary.py
```
实际smoke/串联格式修复的退出情况见下节；最终measure与real命令退出0。

## 输入与一次只读容量（仅canary）

PUB=`D:/SakuraTool/SakuraPool-R2C3-real-20261002/publication`。
content_digest=`23f764d87581fdd2b802e1785947820a5e53647c77a0f9d3cc514299b19a3860`。
snapshot=`329baa6aa8b8b58677bccd2ef3425ac9bf174aecd5594aaecc7d1791db19f6ae`。
实际source gamecg、records121、object1、fetchable rid121/object1。canary不代替生产覆盖；生产P2/P3/Publication完成路径与独立P2/reference NOT_AVAILABLE。

实际stat publication总量 **340,253B，2,812.008B/record**。组件/控制文件精确bytes、百分比、B/record与DBSTAT在canary_metrics.json；主要组件：

|组件|实际bytes|占总量|B/record|
|---|---:|---:|---:|
|runtime catalog.sqlite|184320|54.17145%|1523.30579|
|runtime bitmaps.sqlite|118784|34.91049%|981.68595|
|runtime locations.npy|5057|1.48625%|41.79339|
|remote_objects.sqlite|24576|7.22286%|203.10744|
|image_sha256.npy|4000|1.17560%|33.05785|

控制文件和其余组件共3516B；与上表合计340253B，不把manifest声明值当全部stat实测。runtime已在publication内，不另加独立P3副本。P2 durable缺；非发布物TAR/stage/旧snapshot/cache/wheel/Rusttarget/备份未获明确清单、不广搜或混入下载大小。allocated/RSS NOT_MEASURED，逻辑总量不证明物理占用相等。现有SQLite DBSTAT可用，B-tree、file-minus-Btree差额/freelist分别记录，不能当全部DB容量；未VACUUM/改page size/删sidecar。未压缩，完整archive大小NOT_MEASURED。

## 打开、正式验证与查询

小合成1record先校准计数/bytes/Brecord/batch/计时单位。首smoke测量输出后cleanup退出1：SQLite with只结束事务未close DBSTAT连接；改closing后smoke测量/cleanup成功，同串联Ruff有格式错误，纯import排序/拆长行修后Ruff0，不重复smoke。未改产品生命周期。

canary正式load_publication(full_verify=True)一次含runtime验证，退出0；未额外运行runtime fullverify或CLI verify。正常task API自身必要full检查不绕过，所以不是全流程总计仅一次full。新进程fastopen **.0105443s**；正式full **.0153414s**。新进程不是OS冷盘，不清缓存。

实际catalog取common breasts(cardinality110)、rare tattoo(1)，公开API执行：

|查询|结果数|首次query秒|暖query+count median秒|暖p80秒|
|---|---:|---:|---:|---:|
|all|121|.0000319|.0000017|.0000031|
|common|110|.0001959|.0000628|.0000653|
|rare|1|.0000968|.0000631|.0000646|
|两tag交集|1|.0001426|.0001267|.0001272|
|any_of|110|.0001324|.0001199|.0001210|
|none common|11|.0001045|.0000656|.0000695|
|common且none common合法零结果|0|.0001242|.0001208|.0001225|

暖5次同进程同handle，p80=sorted[3] nearest-rank。first仅query，warm含count，口径不同不能直接冷热比；各spec首次不是独立冷缓存。交/并集合关系与none-common不交及零结果通过；none不是独立全集补集证明，P2/reference缺。未知tag实际UnknownQueryValueError，不规定空集。只有1source，多源NOT_AVAILABLE。record首batch32/.0001569s、location首batch32/.0000333s、固定seed5随机16location/.0000489s。

**当前harness全tags fetchall和每query全set(iter_rids)仅适用本121-record canary**，不能原样称生产有界harness；未来真实规模输入须先限定取样与集合核成本。此轮无生产输入不扩改。无RSS/working set测量，不外推生产查询延迟或规模容量。

## 批准真实小任务

批准旧single-object profile仅内存适配正式task接口，同origin/repo/worker/明确credential reference；不修改原profile、不打印token、不搜替代凭证。先核剩disk约1GiB/inflight256MiB/saved余额，first3 image/JSON真实extents≤8MiB。冻结选择first3相同：image总169039B，JSON另3450B，全部同TAR；MULTI_OBJECT_REAL_VALIDATION=NOT_AVAILABLE，不扩repo、无意义workers4未跑。

|场景/task尾ID|run→close秒|确认交付|全局body增量|全局attempt增量|
|---|---:|---:|---:|---:|
|image w1 /65f67c408c92|43.185764|3|273830|14|
|image w2 /813bd67fb1c7|43.569785|3|273828|14|
|metadata true w1 /dfa594cb14c8|74.180517|3|277278|20|
|metadata closure /a81764c3975c|暂停34.268018 +恢复46.637001|1→3|382071|28|

任务目录固定生产root下p5d-canary-<尾ID>。image任务同plan；metadata任务同另一metadata plan；四任务均first3，实际验证seq0..2。metadata closure首段workers1、新进程resume workers2，**不是连续metadata-w2性能pair**，两个span不相加冒统一E2Ewall或2网络流。job bash_e0fc9d4e真实退出0、scriptRuffPASS。

每task actual verify_delivery、publication imageSHA/长度71128/39402/58509B、seq0..2、attempt3、UNKNOWN0通过。JSON长度1253/1055/1142B、解析dict与交付receiptSHA匹配；无独立publication metadataSHA。fresh process已证，独立freshproof trace未测。

闭环首1 PAUSED、余2 READY；partial1/777B，新进程COMPLETED3，final3/2331B；parsedpartial==final[:1]，非字节prefix。历史pending仅父内存before对子结束after相等，未复制dict文件；每task最终inflight0。全局增量body1207007B/metadata521750B/attempt76/saved_samples12/saved_bytes683056B，与四task对应增量和一致；12个task attempt不等于76个ledger attempt增量。全局disk增量901120B，四task run窗口disk增量和737280B，差163840B：脚本每task initial在create_task之后，建库等未计入各run窗口，不能称四taskdelta覆盖全局或全部物理写入，不重开ledger追补。上述增量仅统计，非逐attempt durable确认receipt或独立HTTP计数。安全原始TASK/FINAL留tool回执，必要摘录已交sole，无新日志。

## 推荐、缺项与安全

WORKERS_RECOMMENDATION=默认1。单TARimage w2单pair比w1慢0.88923%，无统计显著性或升并发依据；same-key affinity限制派发，2/4是上限非流数。closure两run和80.905019s不是E2E墙钟或连续w2对照。忙候选重复prepare、head-of-line、空闲lane预留/ledger槽开销后置，不改缓存/重排/跨laneproof/Range合并/ledger v2。

生产规模门缺完成清单及冻结产物；多源、多TAR、独立reference、allocated/RSS、压缩包均缺证。固定legacy root/caps、8MiB member、通用workspace与安装分发限制仍在，不据此开放发布。仅批准canary真实访问；生产索引未改、历史pending/未知目录不清理；main固定BASE，index机操作0、构建SHA未改。正常dev push完整最终SHA/tree/中间提交及远端核列交付正文。阶段PARTIAL送审，不无限挂起等清单，不自动main合并/上传发布索引/下一功能阶段。
