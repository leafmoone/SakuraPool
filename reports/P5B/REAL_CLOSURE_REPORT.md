# P5-B REAL CLOSURE FIX 完整送审报告

## 身份与范围

```text
P5_B_REAL_CLOSURE = WAITING_REVIEW
BASE_SHA = 4f1aa8a04828716f7b98df8d34d2182a9e287d3b
IMPLEMENTATION_BASE = a7fc37ab8bf511ff46ccd77dedf40acebe39acd9
IMPLEMENTATION_SHA = 836be52abf28877126ee45f3515a964b5c60c71f
IMPLEMENTATION_TREE = 87caed389704510a6ada853b82c41365aaad41c8
SUBMISSION_SHA = 本独立报告提交；完整SHA在最终正文及git rev-parse HEAD给出，不强求自引用
origin/dev = 报告push后实时ls-remote核，应等于SUBMISSION_SHA
origin/main = 479bc3a1caf2b432b5abb643428fdc1b5b6fcbb2
local main = edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a（本轮未改，不同于远端）
KNOWN_HTTP_ACCOUNTING = PASS
UNKNOWN_HTTP_ACCOUNTING = PASS
TASK_KNOWN_REJECT_RESUMABLE = PASS
TASK_UNKNOWN_REJECT_BLOCKED = PASS
SEMANTIC_PROVIDER_PHASE = PASS
FULL_PYTHON = passed1250/skipped7/failed0/wall_seconds999.33/exit0
FRESH_WHEEL = PASS
OLD_BLOCKED_TASK_MODIFIED = NO
P5_B = COMPLETE（限本轮27节闭合，不是用户最终验收或main授权）
P5_C_STARTED = NO
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0
INDEX_BUILD_SHA_CHANGED = NO
REAL_TAR_RESCAN = NO
RUST_CHANGED = NO
```

本轮普通实现836be52abf28877126ee45f3515a964b5c60c71f及本独立报告两持久提交。历史链5ed9004d58720f21cbcf4d011385a4544d8eeb32→a7fc37ab8bf511ff46ccd77dedf40acebe39acd9→23df35cb1c59323b7486376050dd1586af69f432→4f1aa8a04828716f7b98df8d34d2182a9e287d3b保持。实现14paths/491insertions/89deletions，仅显式暂存，无amend/reset/rebase/force。报告只本文件，不夹实现/计划。候选43b/aa2/235/87ca是tree，不是中间commit。原sole reviewer产品235稳定门及87ca tests-only PASS；报告提交后仅送原reviewer证据核，不虚称重测。

开始fetch/status/HEAD/origin exit0 clean/dev/BASE，无漂移。实现普通push后origin/dev实核836be52、origin/main479。本地main未改。旧阶段root漏核“不移动占用分支”导致rename/push preserve-dev-finalize-dirty-de5b311的违规仍按FINAL_REPORT披露，不改称合规；本轮不rename/delete，不动旧dirty/preservation de5b311bf7116e95ec4084f577c7c7cef5147cfa或索引固定SHA57864e5dd456251b457238b196e8fed5668d909b。

## 修复与契约

RemoteIOError显式operation accounting CONFIRMED/UNKNOWN，由实际IO确定全部已发request settle返回证明，不按HTTP码/globalpending/counterdelta/lease消失猜。MetadataBytes携成功read结算证据，ModelScope累计全部证据；任一unknown sticky，成功边界未累计confirmed立即拒绝，不进tree/proof/range/SETTLED；普通bytes默认UNKNOWN。knownstatus/encodingreject、完整boundedread后的JSON/shape/absence/incomplete、已settled retrydelay错误确认；connection/body ambiguity/settle失败未知。

NOT_PUBLISHED+CONFIRMED→itemREADY/accountingCONFIRMED/code原safe，taskBLOCKED、本run停止，只explicitresume再领取；UNKNOWN仍BLOCKED_ACCOUNTING。新claim重置本itemaccounting但保历史attempt证据。REQUEST/Range/rename结算未知和六crash恢复契约不放宽。

固定safephase provider_repository_request/provider_tree_request和shape枚举，公开不泄URL/query/id/rev/token/cookie/body。numeric legacyhub route/Revision pinned/Rootgc5m/RecursiveTrue/Page1/Size200离线match，未改route/root/revision/object/repo。synthetictoken离线无authcookie/有authcookie仅exact schemehostport origin、CDN无、scheme拒通过。

新增离线realledger repo200valid1584B→tree400两attempt/settledmeta1584/newpending0/publichttp400+treephase+CONFIRMED/READYknown+explicitresume；treebodyambiguous对照UNKNOWN/BLOCKED/resume禁。首次connectionunknown→repo retry200在repository提前阻断，预备合法tree未调用；ACCOUNTINGUNKNOWN/UNKNOWN_COUNT1/DELIVERED0/PENDING1/noSETTLED。validplainrepo/tree和priorunknown→plainbytes均阻；plainreadpending0不推confirmed。JSONshape/absent/incomplete/settlefailure/invalidRetryAfter对照覆盖。

## 实际验证、命令、失败

最终绑定87ca；keeper3.13.15/sourceassert、明确worker D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe、unsetCARGO_TARGET_DIR/PYTHONPATH、前后tree检查。Rust源码协议未改，不重build，bridge/production/R2C2集成实跑。

| 验证 | 结果/时间 | exit |
|---|---|---|
| 初复现closure | 4failed19.31s | 1 |
| closure负例 | 12passed44.00s | 0 |
| 首组合 | 15failed387passed3skipped624.86s | 1 |
| 修旧diag/duckprovider定向 | 15passed131deselected1.52s | 0 |
| 43b组合 | 402passed3skipped609.50s | 0 |
| sticky定向 | 3passed12deselected11.08s | 0 |
| aa2组合 | 405passed3skipped633.39s | 0 |
| plainbytes定向 | 17passed180deselected27.03s | 0 |
| 235组合17files | 509passed3skipped658.07s | 0 |
| 首full235 | 16failed1234passed7skipped1523.42s | 1 |
| tests-only定向 | 17passed26deselected0.26s | 0 |
| 最终full87ca | 1250passed7skipped0failed999.33s | 0 |
| Ruff/diff/index/tree | PASS beforeafter87ca一致 | 0 |
| freshwheel | build成功，首import缺pyarrow预检失败，未开始tests | 1预检 |
| 修临时依赖后freshwheel6files | 166passed0skipped409.43s/pipcheckPASS | 0 |
| credentialed真实闭环及离线交付复核 | PASS | 0 |

命令：`python -m pytest tests -q -p no:cacheprovider --tb=short -rs`，定向相同flags；`python -m ruff check .`；`git diff --check`/`git diff --cached --check`/`git diff --exit-code`/`git write-tree`。`python -m build --wheel --no-isolation --outdir C:/Users/PC/AppData/Local/Temp/sakurapool-real-closure-wheel`，临时venv `pip install --no-deps --ignore-installed <wheel>`；补装固定pyarrow18.1/numpy2.2.6/pyroaring1.1/requests2.32.5/pytest8.3.4。仓库外`env/Scripts/python.exe -I -m pytest <test_p5b_real_closure/publication/task_runner/task_resources/task_multiple/r2_production绝对路径> --rootdir=<仓库外目录> -q -p no:cacheprovider --tb=short -rs`。noneditable/direct_url/sitepackages来源/noPYTHONPATH断言、pipcheck通过，涵known/unknown/resume/session/taskworkflow/旧admin。系统sitepackages未提供依赖，仅修临时env，产品无变。

最初测试fixture非loopbackhost/错误allow_loopback各4fail7.18s/7.13s，修测试配置；中间3pass1fail32.63s测试把status含未知reservation当settledbody，改used/pending分离。首组合11旧strictdict缺新accounting、4duckprovider无helper，改modulehelper无证据unknown；full16fail为14typed+2CLI strictdict只tests改。review发现unknownretry成功与plainbytes洗白后各补fault修，不套旧候选PASS。完整7skips为5Windows symlink权限、1questionmark文件名、1native getlimitabsent；组合3为tasksymlink/prereqsymlink/questionmark，无worker缺失。失败不报PASS；合成测试不是生产性能。

## 真实凭证及闭环

已核 reports/R2C3/REPORT.md第9/67行历史PASS与批准profile，已知 D:/SakuraTool/SakuraPool-R2C3-real-20261002/profile.json origin/repo/worker一致，批准reference存在/plainentry通过。仅该reference由connect_profile正常读取，不搜索替代、不打印内容。CONTROLLED_DIFFERENTIAL，不将旧匿名400归因auth。

```text
NEW_REAL_TASK = D:/SakuraTool/SakuraPool-P4-work/p5b-real-closure-d58c1122329e
TASK_ID = 09ad641d65fe40c6a46d9f6459b5250d
PUBLICATION_ID = 329baa6aa8b8b58677bccd2ef3425ac9bf174aecd5594aaecc7d1791db19f6ae
PLAN_DIGEST = e4e2b5a77609bf840a50e23f0b89883336838f55d459a3a783513b45132c3b69
SELECTION_DIGEST = fd306271490c0750d83ff7b6431d72df083100cc4cf71a8908e6098979182bb0
REAL_PROVIDER_REPOSITORY_LOOKUP = PASS
REAL_PROVIDER_TREE_LOOKUP = PASS
REAL_PROVIDER_TREE_HTTP_STATUS = NOT_CAPTURED（正常reader返回代码门要求200，未独立捕获trace）
REQUEST_SHAPE_OFFLINE_MATCH = YES
CREDENTIALED_TREE_HTTP400 = NO（本轮成功，无400观测）
REAL_ACCOUNTING = CONFIRMED
REAL_FIRST_RECORD = PASS
REAL_PAUSE = PASS
REAL_PARTIAL_EXPORT = PASS
REAL_RESUME = PASS
REAL_SECOND_RECORD = PASS
REAL_FINAL_EXPORT = PASS
REAL_DELIVERED_CONFIRMED = 2
REAL_UNKNOWN_ACCOUNTING_COUNT = 0
```

同pub fullverify/pinPASS，repo leafmoone/webdataset_danbooru_v3/httpsmodelscope、first2/metadatafalse/maxoutput8MiB。lookup依据正式无externalcontrol exact_provider_lookup正常返回，hub/tree/size/pinnedrevision/providerdigest门后到ownproof及image；不是独立status抓取，不造实际trace200。

record1SETTLED hook写guardedPAUSE→PAUSED delivered1 unknown0；record2READY/attemptNone实assert。partialexport1/623bytes。关闭旧transport，freshRustProductionTransport+PublicationSession explicitresume freshlookup/ownproof→record2→COMPLETED delivered2/unknown0，finalexport2/1246bytes。freshproof依据实际verify_conditions路径与durableSETTLED，不globalproofdelta。

冻结seq记录69313e4c9810d6a4f5adb45980b54a17、fd750c9f949645e489e86f6da99c339e；额外仓库外-I无网络只读assertpartial==final[:1]/finalorder==TaskDBseq/attemptcount2/每image长度manifestSHA==publicationmmapridSHA。image71128+39402=110530，无metadata输出，不重复saved。旧p5b-real-task-3456f97a35c9不写/删/退款/重解释，仅最终ro仍BLOCKED/unknown1，历史证据保留。

## 正式ledger before/after

| 字段 | before | after | delta |
|---|---:|---:|---:|
| attempts |139|159|20|
| settled body |11746046|12066154|320108|
| total body含历史unknown reservation |11746062|12066170|320108|
| metadata |1194714|1403414|208700|
| saved samples |2|4|2|
| saved bytes |144762|255292|110530|
| disk |1862336512|1862500352|163840|
| records |242|242|0|
| inflight |0|0|0|
| pending count |16|16|0|
| proof count |3|3|0|

proofbefore实际记录，可列after但不作为ownproof依据。历史pending完整字典原行相等，新pending0；taskunknown0，历史16unknown保不退款。delta仅统计不确认单attempt，IO/回执自己证明。旧报告disk1799299072到本before1862336512的变化未归因本真实任务，不编造原因。

## 限制与停止

P2/P3/publication4/2/2、Rust/ledger格式/caps不变；knownrejectREADY是本轮批准契约变化，unknown未放宽。串行单runner/Range8MiB/legacybudget/跨平台限制仍在，Windows部分skip。无新TARscan/索引/大规模任务；保新task旧blocked及历史账本。owned短期env核无reparse后回收、wheel保留，keeper/worker/旧dirty/分支不动。报告push后实时SHA核并送原reviewer仅证据检查；完整正文止WAITING_REVIEW，不main、不C。
