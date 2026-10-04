# P5-D 最小收敛与可用输入验证报告

## Pinned optional-entry compatibility 与原任务恢复（当前有效结论）

```text
P5_D = WAITING_REVIEW
BASE_SHA = 1354a07ea8d66aa912d1d8a72c7b88607501e31f
PINNED_EMPTY_ENTRY_REVISION = PASS
MASTER_CANDIDATE_BEHAVIOR = PASS
EXACT_PROVIDER_DIGEST_CHECK = PASS
TARGETED_REGRESSION = PASS
EXISTING_TASK_RESUMED = YES
HISTORICAL_FAILED_ATTEMPT_PRESERVED = YES
REAL_THREE_SOURCE_THREE_OBJECT_TASK = PASS
REAL_METADATA = PASS
REAL_PAUSE_RESUME_EXPORT = PASS
DELIVERED_CONFIRMED = 3
UNKNOWN_ACCOUNTING_COUNT = 0
PREVIOUS_PENDING_COMPARISON = NOT_COMPLETED
THIS_RESUME_PENDING_COMPARISON = PASS
P2_P3_PUBLICATION_REBUILT = NO
CAPACITY_AND_GZIP_REMEASURED = NO
P5_D_VALIDATION = COMPLETE_FOR_FROZEN_SCOPE
VALIDATED_SCOPE = 20261004T143905Z-upload
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0
GLOBAL_INDEX_BUILD_COMPLETE = NOT_CLAIMED
```

当前用户明确批准新兼容契约替代上轮resolved-echo不足阻断策略。请求revision仍仅master或合法完整40/64位commit，短/非法在HTTP前拒绝。master必须合法entry candidate；pinned entry Revision空字符串/null/缺失可解析，合法same/different不覆盖request pin，非空非法type/format拒绝。ListedFile.revision_candidate与download URL始终同request pin。entry为可选metadata，不声称有不存在的resolved commit回显；provider独立内容摘要/path/size一致性和运行时fresh strong ETag If-Match、Range及独立image SHA仍须通过，未退master/latest、换repo/version或猜revision。

### 定向离线证据与候选身份

本轮先核statusclean/dev与origin1354、maina9c。实际Python3.13.15，导入D:/SakuraTool/SakuraPool-p5a-clean/src/sakurapool/__init__.py（当前源码，无旧wheel），复用keeper/worker，无Rust重建。委派产品回执称定向149passed/12deselected26.00s，但未保留完整命令/过滤条件，无法准确确认12deselected原因，不补造；此数仅背景，不作为可复核统计。为补足直接证据，主代理执行 `$PY -m pytest -q tests/test_legacy_tree_typed.py tests/test_legacy_pinned_lookup.py` exit0：103passed0.28s/no deselected。此前误带不存在的tests/test_legacy_control_plane.py命令exit4/no tests ran，已纠正路径；未扩历史矩阵或重网。新增空/null/缺失40/64pin及合法不同entry、非法fields、master/短request、整页可定位与digest阻断均覆盖。另命令 `$PY -m pytest -q tests/test_task_runner.py::test_pause_request_then_explicit_resume tests/test_task_resources.py::test_unrelated_historical_pending_does_not_block_confirmed_task tests/test_task_runner.py::test_range_exit_settlement_failure_never_confirmed` exit0：3passed21.67s。

harness原4项内存合成覆盖主异常保留、finally审计、历史attempt行变更拒绝及错误脱敏；收口另5 faultcases：主异常+after失败、主异常+TaskDB失败、主异常+metrics和print同时失败、无primary成功路径metrics+print失败保原metrics错误、原AssertionError+metrics/print失败保原AssertionError，均exit0。必要读取失败新比较NOT_COMPLETED，不掩原异常；close失败只safe固定worker_close secondary。无全pending/historyrows落盘或额外hash。

sole产品有限核与harness集中候选49dd00c3f1619bcb0da3c724262dfa58645dc7c3通过；最后授权最小修后实际网络执行tree `35747892e94b46677a6b3cef03267434b296394a`。最后报告/指标变更不重新制造认证，最终commit/tree交付正文列实际值，不自引用。

### 原三source/三TAR真实闭环

实际命令：`D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -u reports/P5D/real_multi_object.py --continue-existing D:/SakuraTool/SakuraPool-P4-work/p5d-fixed-multi-96e9c394114e`，job bash_b996690e exit0。

只读预检验证已有plan/contentdigest/snapshot与冻结selection一致；原首READY/CONFIRMED无交付、余两READY/NONE未运行、UNKNOWN0、无输出冲突且预算满足。未手工改状态或删attempt/pending。正式resume=True workers1，首SETTLED请求PAUSE：PAUSED/delivered1、余2READY，49.134898s（run+close）。partial export1/788B。新Python进程、新transport/session workers2 resume：COMPLETED/delivered3/UNKNOWN0，72.619057s（run+close），final3/2347B；冻结seq0,1,2/record唯一，partial为final前缀。workers2为上界不宣称实测双lane并发。

|seq/source/object|image B|独立pub imageSHA+extent|JSON B|JSON验证|
|---|---:|---|---:|---|
|0 anime_pictures/object0|165548|PASS|5086|extent+dict解析+delivery receipt PASS|
|1 bangumi/object8|340319|PASS|4131|extent+dict解析+delivery receipt PASS|
|2 konachan/object8352|909963|PASS|3794|extent+dict解析+delivery receipt PASS|

原失败attempt1逐行保持；新增attempt3/total4，非硬编码总3。全部3item DONE/PUBLISHED/CONFIRMED，UNKNOWN items/attempts均0，无重复累计交付。全局ledger本次delta body2120681/meta690523/disk1454080/attempt44/saved_samples3/saved_bytes1428841，inflight前后0；全局网络attempt44不同于任务attempt4，不作为交付receipt。before pending仅父内存、本次after相等PASS，历史比较永久NOT_COMPLETED不追认。finally after与TaskDB读取成功，secondary_errors=[]。

证据新owned `D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/real-multi-object-resume-metrics.json`；exports在原task/partial.jsonl、final.jsonl，交付在原task/output。未重扫TAR/解包/编译/Pub/gzip/容量/全query；未索引机操作、上传或可见性变更。COMPLETE_FOR_FROZEN_SCOPE仅汇合下方已有输入/P3/Pub/有界query门及本次真实门，独立语义参照仍SUBSET_ONLY，无全局索引完成、生产吞吐或新增多版本/Rust认证声明。

## 固定生产快照续验证历史（上轮 PARTIAL 时点）

> 下节旧BLOCKED、parser未放宽、不resume等陈述保留上轮事实，已由上方用户批准兼容契约和本次真实恢复替代；旧pending比较未完成仍永久成立。

```text
P5_D = WAITING_REVIEW
P5_D_VALIDATION = PARTIAL
VALIDATED_SCOPE = 20261004T143905Z-upload
SNAPSHOT_INPUT = PASS
P3_COMPILE = PASS
PUBLICATION_BUILD = PASS
PRODUCTION_SCALE_QUERY = PASS_WITH_SUBSET_REFERENCE
REAL_MULTI_OBJECT_TASK = BLOCKED
BLOCK_REASON = SOURCE_REVISION_BINDING
GLOBAL_INDEX_BUILD_COMPLETE = NOT_CLAIMED
HISTORICAL_PENDING_COMPARISON = NOT_COMPLETED（本次失败任务）
MAIN_MODIFIED = NO
```

本节替代下方初始canary阶段的生产NOT_AVAILABLE停止条件；下方保留历史验证与失败事实，不将旧canary改称生产。续验证起点dev/origin-dev为 `e14847bf0f76a926e63d855fdbcfe14295627dd7`；先前实现提交 `e7eb75016659ea411c6bec9fc35a8c2d70fc6697`，main仍为 `a9c93f8283573915633c8f70099e7f88267f2ea6`。本轮最终提交在交付正文列实际完整SHA，不自引用。

### 冻结输入与正式编译

管理员资产根 `D:/SakuraTool/SakuraPool-P5D-20261004T143905Z`。仅批准私有索引repo `leafmoone/sakurapool-index` 的固定revision `3473d52b17cc55fb4906fdc13225fb86e042b7f8`、prefix `snapshots/20261004T143905Z-upload/` 下10文件：GLOBAL_REPORT及每机器PACKAGE、PACKAGE-VERIFIED、archive，共7清单+3包，不含旧103 checkpoint。SDK正规下载expected_sha256逐包来自PACKAGE，job bash_dbd17ab3 exit0；实际stat与PACKAGE/VERIFIED/SDK sizes相等。SDK实际archive校验与extract仅比较两个manifest SHA的代码分开，不声称extract重新计算archive SHA，不生成新hash清单。

|机器|archive B|本地members|本地P2 files|本地P2 B|
|---|---:|---:|---:|---:|
|11311|1456692819|13784|13526|1831779316|
|12435|1651437251|14761|14490|2070308407|
|12436|1674277469|14925|14651|2096653935|

stream逐P2 member实际bytes/SHA与既有SNAPSHOT_REPORT清单核全部PASS，保留machine前缀，无extractall；成员43470与P2文件42667不同。实际完成267根集合与report严格相等。正式load_p2_inventory逐根验证fragment完整性/schema/行归属/lineage/count/errors，正式combine再核身份冲突拒绝，无静默去重；objects8480，samples/annotations8782107，errors0（各根显式0，不将Counter省零当证据）。source覆盖：anime_pictures 8objects/5150samples；bangumi2552/3538571；danbooru5792/5196037；konachan104/33377；yande24/8972。source/post_id相同不表示物理去重。source fingerprint `d628176495e552688a7f173bd134664d7aac383909d093f5ba54a336d715815b`。

loader+combine351.4407698s；正式JSON roots wrapper再次load193.3678275s（前进程inventory已结束，未绕过），compile4495.2005093s分别计，不相加冒编译时间。snapshot `226a63935e73dc3d628024615425ad9d75b985686646d15dec151aec205feede`；rid8782107/objects8480/source5/dataset5/tags817929/memberships248465182。管理员根p2-roots.json、p2-validation.json、p3-compile.json保存必要结果。

Extractor后续安全修采用逐层lstat拒symlink及Windows全部reparse属性，再mkdir/xb；合成普通目录、非目录、6种危险Windows路径、mock symlink/reparse前置拒绝检查通过。原实际解包版本在mkdir之后检查symlink，不含junction；当时未独立记录祖先无reparse/并发修改证据，不能以事后修补追认。修正版仍依赖独占owned目录，不宣称对恶意并发替换race-proof；未重新生产解包。

### 原图映射与Publication

官方dataset Git ls-remote当时HEAD/master精确匹配授权短73306f1d，扩展为 `73306f1dc5459238710f477b376c36da997d020c`；后续所有SDK相关prefix listing固定该完整SHA，可变master不是下载pin。19557 metadata entries仅元数据；runtime8480 objects按dataset+object_path匹配官方独立Size/Sha256，8480全部一致，missingmetadata/explicitSHAabsence/sizeSHA mismatch均0；没有抄P2 SHA/null掩冲突，没有原TAR下载或8480proof。

正式build新目录publication，743.7432517s exit0；内置P2 loader/combine及runtime full verification如实执行，不绕过不另重复认证。Publication映射/结构定义8480objects/8782107RID fetchable（仅可寻址映射覆盖，不是provider fresh proof通过或实际下载成功）；实际publication绝对路径 `D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication`；durable_included=false/runtime-first，不含P2。content digest `c186f0a93d9bcf036f54a926ddae3b43aef35d6cfac4eb6dae2151abebcbd0eb`。provider-metadata.json、remote-map.jsonl、publication-p2-roots.json均仅管理员根。

### 实际容量与查询

publication12files总2204511811B，251.02310994B/record：catalog1026129920、bitmaps551206912、locations342502511、imageSHA281027552、remote_objects3641344；控制/其他PUBLICATION1194、READY64、runtime/current240、OWNER488、runtimeREADY64、SNAPSHOT1517、STAGE5。runtime已含1919841657B，不再叠独立runtime副本。P2正式5998741658B/42667files，全部extracted含证据6020561957B/43470files；download10files4782411532B含3archives和7清单。实际逻辑stat不扫描内容。allocated NOT_MEASURED：GetCompressedFileSizeW返回值均logical，仅APIreportedstoredbytes，不当clusterallocated；RSS NOT_MEASURED。

唯一完整本地publication.tar.gz归档已测：1336756276B，94.2198268s，标准gzip6/tar.gz仅publication12files，job bash_e6ca673c exit0；管理员根新路径确认不存在后创建，未覆盖既有文件。预检D盘free320073572352B。该单写入方使用tarfile w:gz（非OS exclusive open，依赖owned目录无并发写入假设），不是公开上传/生产下载吞吐，不新增hash或多档对比。

仓库合并frozen_scope_metrics.json中的production.capacity.p2规范为NOT_MEASURED_BY_THIS_HARNESS，仅纠正说明字段；原管理员production-metrics.json的NOT_AVAILABLE保留，其余测量未重跑。

production-metrics.json为修前既有输出，capacity.p2旧NOT_AVAILABLE不能表示P2不存在；新脚本改NOT_MEASURED_BY_THIS_HARNESS，上述P2来自已完成验证，未为措辞再full。DBSTAT现有可用，三SQLite file-minus-Btree0/freelist0，详细btree在metrics；无VACUUM/page变更。

新进程fastopen0.0141276s；本次正式pubfull7.1320356s含runtime，build和task API亦有必要检查，不冒全流程一次。first和warm均query().count，warm5同handle，p80 sorted[3]；OS cache未控制，无统计显著性宣称。

|query|count|first s|warm median s|warm p80 s|
|---|---:|---:|---:|---:|
|all|8782107|.0000720|.0000164|.0000174|
|common danbooru_native/1girl|3618837|.0013780|.0002294|.0002821|
|rare anime_pictures_native/belarus_(hetalia)|1|.0001269|.0000623|.0000650|
|intersection|0|.0001716|.0001705|.0001731|
|any_of|3618838|.0004250|.0003307|.0004664|
|none common|1577200|.0017599|.0008254|.0008855|
|contradictory zero|0|.0011435|.0014413|.0017544|
|anime_pictures+bangumi sources|3543721|.0017130|.0001832|.0002542|

none作用在对应known namespace，旧none+common==all断言已纠正为namespace-known，不改产品。补测5namespace有限catalog LIMIT17/每namespace tag LIMIT1，不重full/容量：anime_pictures_native girl4331/none819/known5150；bangumi_native bangumi3538571/0/3538571；danbooru_native1girl3618837/1577200/5196037；konachan_native long_hair13736/19641/33377；yande_native thighhighs2079/6893/8972；每zero0。只catalog/bitmap count关系，不冒独立全库语义。unknown tag抛UnknownQueryValueError。首record/location batch32为.0002310/.0000590s；seed5随机16locations .0000699s。

独立P2参照每dataset一个object首annotation batch<=64，实际257行（danbooru所选object仅1），5dataset/13spec×257RID成员判断与独立P2 namespace/known/empty/tag/source语义一致；3485是有category的annotation tag entry逐项catalog存在检查次数，含重复，不是唯一tag/category数。通过私有_catalog/_bitmaps及QueryResult._bitmap contains辅助，不称纯公开API参照；只保最多257RID，不全queryset物化。sample all257/common1/rare0/intersection0/any_tags1/any_of1/none0/zero0；source各anime64,bangumi64,danbooru1,konachan64,yande64。rare/none/intersection样本0不独立证明全库零或正例；PASS_SUBSET_ONLY，不全记录比较。

### 新scope真实任务阻塞与安全收口

P4准入剩disk1071210496/body8576178005/meta64870650/inflight268435456/attempt1735/saved981。选择三source三TAR实际extent：anime rid0/object0 image165548+JSON5086；bangumi rid5150/object8 image340319+JSON4131；konachan rid8739758/object8352 image909963+JSON3794，metadata flag1且分别<8MiB。冻结选择管理员根real-task-selection.json；复现real_multi_object.py，未改旧real_canary.py。

真实job bash_1b49fcb8 exit1，task `D:/SakuraTool/SakuraPool-P4-work/p5d-fixed-multi-96e9c394114e`，provider_entry_revision_shape。requested3/delivered0/error1/UNKNOWN0/stateBLOCKED；首itemREADY/NONEdelivery/CONFIRMEDaccounting，余2READY/NONE；attempt1 NETWORK_START/nooutputlease/no receipt。当前inflight0。运行前后ledger增body64925/meta64925/disk40960/attempt2，saved不增，仅全局统计非交付证明。收口修正复现脚本尚未执行到的成功分支：resumed.result.state断言由DONE改为COMPLETED（task状态，不是item状态）；未重新网络验证该分支，不影响既有provider阻断根因。未达到pause/export/freshresume、未完成JSON输出验证，无2活跃lane宣称。real脚本仅成功末尾写real-multi-object-metrics.json，异常路径未写最终after汇总；本次after与TaskDB状态另由只读诊断取得，不能当脚本成功闭环。异常退出使父内存pending比较未完成，永久NOT_COMPLETED，不能事后新baseline追认；不删task/pending，不自动resume/重建。

独立mapping metadata一致不代fresh own proof。最小正规官方metadata诊断：精确repo numeric218254/Type4；GET https://modelscope.cn/api/v1/datasets/218254/repo/tree，完整fixed73306...、Rootanime_pictures/RecursiveTrue/Page1/Size20，HTTP200，20blob Revision全空（missing/null/invalid/valid40各0）；响应顶层及Data无resolvedfullSHA/CommitId。此前19552blob+5tree全Revision空。源码entry.Revision为文件commit，正式门要求40hex。请求SHA/sizeSHA/ETag不足source revision binding；无可靠独立响应绑定，保持BLOCKED，不放宽parser/master门、不循环猜参数，不改产品。诊断不混入上述任务delta。

相关publication tests57passed53.51s exit0；bounded1rid smoke exit0，旧tagged121canary只读校准exit0无新下载，不替新真实任务。extract合成回归/diff/cachedcheck exit0。sole集中tree c206后最小delta最终tree7c61dfe6504acf1458b17cf8b871d26a04a04594 PASS；没有Rust/C全回归/310/wheel/stress重认证。脚本改变的是测量有界性/安全提取，不改publication格式、预算、pipeline或provider契约。默认workers1不提升，显式2/4非网络流保证。

管理员根必要证据完整路径：production-metrics.json、p2-subset-reference.json、namespace-metrics.json、provider-identity-diagnostic.json、capacity-metrics.json。仓库报告与精简metrics补充，不以链接替交付正文。管理员资产构建空间与P4预算分开，无main/index机/原图库修改、上传/可见性变更、全TAR扫描、生产ledger迁移、pending退款或未知文件清理。仅冻结scope验证PARTIAL，GLOBAL_INDEX_BUILD_COMPLETE NOT_CLAIMED；最终普通commit/push dev核远端，停止WAITING_REVIEW。

### 实际执行命令 / 退出码摘要

工作目录 D:/SakuraTool/SakuraPool-p5a-clean；PY=D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe，实际Python3.13.15，PYTHONPATH指当前src。以下为已执行结果，不重跑：

|实际调用|exit|结果/job|
|---|---:|---|
|$PY -m pytest tests/test_publication.py -q|0|57passed/53.51s dc38c13a|
|$PY reports/P5D/measure_publication.py --smoke|0|1rid bounded smoke42066703|
|$PY reports/P5D/extract_snapshot.py|0|三包member完整性45a4cb3f；后续安全修仅合成测试|
|$PY reports/P5D/p2_subset_reference.py|0|257sample/13spec|
|Python inline wrapper load/combine(JSON roots)|0|13ce34cb/351.44s|
|Python inline wrapper load_p2_inventory+compile_runtime|0|1711e640/load193.37s+compile4495.20s|
|Python inline wrapper build_publication|0|d7583bdc/743.74s|
|Python inline import measure_publication.measure(newpub)|0|43668fc7/production-metrics|
|Python inline bounded namespace query wrapper|0|5namespace count关系，无fullverify|
|$PY -u reports/P5D/real_multi_object.py|1|1b49fcb8/身份门BLOCKED|
|Python inline fixed numeric metadata diagnostic|0|HTTP200缺resolvedrevision|
|Python inline tarfile gzip6 archive|0|e6ca673c/1336756276B|

compile/build/namespace诊断为实际inline wrapper，不虚造独立脚本命令；必要输出与身份在管理员根证据。最终Ruff首次exit1仅导入排序/行长，后续纯格式修及最终命令结果交付正文列实际退出码，未重真实任务。

## 初始canary阶段历史：状态、范围与身份

> **历史区段说明：以下全部“当前、未来、缺输入、main BASE”等措辞均描述初始canary验证时点，不描述上方固定生产快照续验证。历史NOT_AVAILABLE与旧harness限制已由上方实证/有界脚本替代，旧任务结果不重新标为生产规模结果。**

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
