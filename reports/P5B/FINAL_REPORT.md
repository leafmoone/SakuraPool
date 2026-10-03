# P5-B 完整送审报告

## 范围、提交与停止边界

仅完成授权 P5-B 可恢复串行下载任务及其依赖的错误/清理语义修复。24 个实现/文档/测试路径，2643 insertions、25 deletions。sole reviewer 对最终实现 tree 稳定认证门 PASS；不代表用户阶段验收通过。真实任务确定性服务错误 BLOCKED，不能伪报 PASS。报告独立提交，不再改实现/计划。

```text
P5_B = WAITING_REVIEW
BASE_SHA = 479bc3a1caf2b432b5abb643428fdc1b5b6fcbb2
FIX_COMMIT_SHA = 5ed9004d58720f21cbcf4d011385a4544d8eeb32
IMPLEMENTATION_START_SHA = 5ed9004d58720f21cbcf4d011385a4544d8eeb32
IMPLEMENTATION_END_SHA = a7fc37ab8bf511ff46ccd77dedf40acebe39acd9
IMPLEMENTATION_TREE = 8aeb99625f3eebea7d0d2c63b518eea639d25079
SUBMISSION_SHA = 本报告独立提交；完整SHA在最终回复及git rev-parse HEAD
origin/dev = push本报告提交后核验，应等于SUBMISSION_SHA
origin/main = 479bc3a1caf2b432b5abb643428fdc1b5b6fcbb2
```

全部持久提交：前置修复5ed9004d58720f21cbcf4d011385a4544d8eeb32；实现a7fc37ab8bf511ff46ccd77dedf40acebe39acd9；本报告独立提交。实现已普通push，远端dev实核实现SHA，main实核基线；报告push后的最终完整SHA在最终回复核验。候选741a4ab/34cf4ab/8d6708c/8aeb996是tree，不是中间提交。无amend/reset/rebase/force push，未自动合并main，未启动P5-C。仅显式暂存本轮文件，数据/TAR/任务/环境/cache不入Git。

## 契约与验收矩阵

```text
SQLITE_URI_ENCODING = PASS
FILTERED_EMPTY_PAGE_CONTINUATION = PASS
PAGINATION_LIMIT_VS_ABSENCE = PASS
PRIMARY_ERROR_AND_CLEANUP_STATE = PASS
FROZEN_SELECTION = PASS
SELECTION_CONTENT_DIGEST = PASS
TASK_ID_VS_PLAN_ID = PASS
TASKDB_ATOMIC_STATE = PASS
SINGLE_RUNNER_OWNERSHIP = PASS
SERIAL_EXECUTION = PASS
PAUSE_CANCEL_RESUME = PASS（离线和安装wheel；真实未到此阶段）
VERIFIED_SUBSET_EXPORT = PASS（离线和安装wheel；真实未到此阶段）
CRASH_RECONCILIATION = PASS
ACCOUNTING_UNKNOWN_FAIL_CLOSED = PASS
NO_DUPLICATE_CONFIRMED_DELIVERY = PASS
ACTIVE_BUDGET_PROFILE = P4_LEGACY
LEGACY_BUDGET_LIMITS_EXPLICIT = YES
GENERAL_WORKSPACE_BUDGET_MIGRATION = NOT_IMPLEMENTED
UNCONDITIONAL_EXACTLY_ONCE_CLAIM = NO
P2_FORMAT = 4
P3_RUNTIME_FORMAT = 2
PUBLICATION_FORMAT = 2
LEGACY_PACKAGE_COMPATIBLE = YES
MAIN_MODIFIED = NO
INDEX_MACHINE_OPERATIONS = 0
INDEX_BUILD_SHA_CHANGED = NO
P5_C_STARTED = NO
```

- CLI create/inspect/run/pause/cancel/resume/export；create/inspect不取token、不访问provider；收到控制请求不冒称已停止。
- 冻结all/first/sample/records选择，保留查询域及同post多source；digest绑定seq/rid/record_id/source/dataset/post_id；resume不重新查询/采样。独立task ID和plan ID。
- SQLite普通journal/FULL sync/短事务，事务内不做网络；真实OS文件锁确保单runner。
- create父目录plain-entry/formal-root/预算准入先目录/SQLite写；run/resume/control先准入再RW SQLite；inspect只读。TaskDB是内部primitive，不扩本阶段Python control API。
- profile origin/repository/worker/credential-reference白名单；有限安全diagnostic/secondary，不回显秘密。
- export仅确认并验证的子集，不覆盖外来文件；manifest必须在TASK目录，output相对路径与基准一致。
- 无durable receipt的settle窗口保守BLOCKED/UNKNOWN；可信durable SETTLED后、finish前恢复零重下。receipt绑定完整attempt/operation/seq/plan/output及相关结算，不用global pending/lease消失确认。
- transfer/probe先pin artifact ownership；未知/替换文件保pending，不产生proof/cache/register/SETTLED。cleanup/settle/rollback/release/DBclose/unlock/sessionclose不盖原primary；无primary时首次中断正常传播。
- 保持legacy saved_samples1000/saved_bytes536870912/body8589934592/attempts2000/disk4294967296/metadata67108864/records100000/inflight268435456，历史16 pending不迁移、不清理。
- 冷路径已知mandatory下界含6个proof HTTP hops，加metadata lookup和Range；后续分页各自受预算门控制，不宣称精确总网络成本；delivery/content/lease/op-network各自结算。

## 最终实现tree的实际验证

所有最终验证绑定8aeb99625f3eebea7d0d2c63b518eea639d25079，串行使用明确worker D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe，清除继承CARGO_TARGET_DIR。Rust源码/协议未改，复用已有认证worker；执行相关Python bridge/production集成，不新增Rust重编/Clippy声称。

| 范围/命令 | 实际结果 | exit |
|---|---|---|
| 9 task文件+test_r2_production.py+test_publication.py，pytest -q -p no:cacheprovider --tb=short -rs | 187passed/1skipped，409.01s | 0 |
| keeper3.13.15，python -m pytest tests -q -p no:cacheprovider --tb=short -rs | 1232passed/7skipped/0failed，1437.18s | 0 |
| python -m ruff check . | All checks passed | 0 |
| git diff --check；git diff --cached --check；git diff --exit-code；前后git write-tree | 工作树=index，前后tree相同 | 0 |
| Python3.10.21最新源码，上述11文件+test_p5b_prerequisites.py，version/source/worker核验 | 212passed/3skipped/0failed，415.42s | 0 |
| keeper python -m build --wheel --no-isolation --outdir <短期环境>/fresh-wheel | 新sakurapool-0.1.0-py3-none-any.whl构建成功 | 0 |
| 临时环境卸editable→pip install --no-deps新wheel→仓库外python -I；site-packages/direct_url非editable/noPYTHONPATH/assert；pip check | 全通过，无依赖问题 | 0 |
| 仓库外3.10.21 python -I -m pytest <上述12文件绝对路径> --rootdir=<仓库外短期环境> -q -p no:cacheprovider --tb=short -rs | 212passed/3skipped/0failed，413.72s | 0 |

Wheel实际import来自 C:/Users/PC/AppData/Local/Temp/sakurapool-p5b-py310-57zes48z/Lib/site-packages/sakurapool/__init__.py。测试文件来自仓库，产品import来自已安装非editable wheel；不是源码安装伪装。覆盖create/run/resume/export、修复负例及旧admin兼容。

完整7skips：local builder/P4 package/prerequisites/runtime remediation/task safety五项Windows symlink权限；一项Windows literal question-mark文件名；一项native getlimit-absent runtime要求。专项3skips是task symlink、prerequisite question-mark、prerequisite symlink。无worker缺失skip。

专项实证：真实双进程锁竞争/释放；六个真实进程crash窗口CLAIMED/REQUEST/PREPARED/PUBLISHED/BEFORE_SETTLED/SETTLED；多对象/session复用；pause/cancel/resume边界；typed namespace/none_tags/any_of/multi-source；foreign/replaced output/journal安全；near-cap零HTTP/RW DB；实际run资源诊断/中断身份+sessionclose/unlock/DBclose三secondary叠加与敏感文本隔离。

### 失败、修正与旧证据界限

初始Windows先读后锁PermissionError修为先锁，真实两进程复验。旧候选1201/7完整、旧310128/3均不套最终tree。审查发现primary cleanup override、RW admission、typed diagnostics、cold预算、export基准、positive proof污染及完整run退栈残门，修改后组合重测、原sole reviewer复审PASS。

最终310首预检exit1 ModuleNotFoundError（测试未开始）；随后PYTHONPATH源码跑210passed/3skipped/2failed413.10s，两个旧admin python -I子进程忽略PYTHONPATH，临时环境没装项目。仅修source环境安装，重跑212/3通过；freshwheel随后卸editable、独立非editable212/3通过。失败轮次不报PASS。真实前预检误用open_publication名称，ImportError无HTTP，改实际load_publication；不改产品代码。

## 真实任务：BLOCKED，不是PASS

离线认证全部完成后使用既有publication，不扫描新TAR。实际匿名profile未配置credential_ref，env token不存在不代表其他凭证来源不存在；未自行搜读秘密。

```text
REAL_TASK_REGRESSION = BLOCKED
TASK_DIRECTORY = D:/SakuraTool/SakuraPool-P4-work/p5b-real-task-3456f97a35c9
TASK_ID = 16f6b4ac16e047cfb60bdc1827f1aada
PLAN_DIGEST = e4e2b5a77609bf840a50e23f0b89883336838f55d459a3a783513b45132c3b69
PUBLICATION_ID = 329baa6aa8b8b58677bccd2ef3425ac9bf174aecd5594aaecc7d1791db19f6ae
PINNED_PUBLICATION_FULL_VERIFY = PASS
CREATE_FIRST_2 = PASS
RUN = BLOCKED：http_status / metadata_headers / HTTP400
DELIVERY = NOT_PUBLISHED
TASK_ACCOUNTING = UNKNOWN（保守fail-closed，不由全局delta推断）
REAL_PAUSE_RESUME_EXPORT = NOT_RUN（未到边界；离线/wheel通过）
REAL_COMMAND_EXIT = 2
KNOWN_NETWORK_CONSUMPTION = attempts+2 / body+1584 / metadata+1584
NEW_PENDING_AND_UNKNOWN = 新global pending0；任务一项保守UNKNOWN；历史pending16→16完整保留
SAVED_SAMPLES_AND_BYTES = 本任务0/0；全局前后均2/144762
NEW_PROOFS = 0
```

Allowlist https://modelscope.cn / leafmoone/webdataset_danbooru_v3；明确已认证worker。first2、metadata=false、max_output_bytes8MiB。计划在SETTLED hook后使用真实guarded控制请求做pause/partial export/resume/final export；HTTP400发生前无SETTLED，不能假称这些真实步骤执行成功。确定性错误停止受影响任务，未盲重试、未绕过UNKNOWN恢复门。

正式ledger before→after：attempts137→139/body11744478→11746062/metadata1193130→1194714/disk1799258112→1799299072/records242→242/inflight0→0/saved_samples2→2/saved_bytes144762→144762；disk+40960是本地task storage，不是图片保存。实际开始remaining：attempts1863/body8578190114/metadata65915734/disk2495709184/saved_samples998/saved_bytes536726150/records99758/inflight268435456。

历史pending实际count16→16，并在内存完整原pending字典逐行比较确认未改变。变量pending0表示before集合，不表示数量0；新pending0是集合差数量。没有迁移/清理历史16行，未以global pending/delta确认本attempt结算。

## 性能、限制与安全

最终源码/wheel的10k选择、跨batch/恢复SQL成本和ledger slot write/fsync合成测试通过。合成小数据不是生产吞吐；最终-q没有打印独立benchmark时间，不把旧候选timing套新树，不承诺倍数。

仍为串行单runner，不支持并行/跨机器锁/P5-C；大member受8MiB Range/任务output和legacy预算限制，未放宽cap。Windows当前有真实锁/中断故障覆盖，但部分权限负例skip，不声称所有OS认证。真实400具体根因未验证，不能断言凭证缺失是原因。

Rust/ledger/P2/P3/publication格式不变；main和索引机不动，不清历史dirty worktrees/数据/账本，不启动大规模任务。短期env已核本轮拥有且无reparse，回收Include/Lib/Scripts/pyvenv.cfg，仅保留fresh-wheel/sakurapool-0.1.0-py3-none-any.whl；keeper和worker未删。本报告-only提交不冒称重跑所有测试。停止P5_B=WAITING_REVIEW，真实限制待外部审查，不自动合并main或进C。
