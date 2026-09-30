# Python 版本策略与唯一项目 venv · 独立变更附录

状态：验证与授权清理完成，文档/证据提交正常 push 后停止于 WAITING_REVIEW。
本附录与最终连续交付正文配套；历史 Final Fix3 `reports/R1-final/` 原字节保持，不以旧认证替代本次认证。
本附录所在提交的 SUBMISSION 由 `git log -1 -- reports/Python-venv-20260930/report.md` 得到；自己的内容不能嵌入自身 Git SHA，最终交付正文列完整 SHA/tree 与远端核验。

## 1. 身份、范围与契约

- 本轮起点 / 历史 Fix3 SUBMISSION：`66579d7f9e50575cefd17c6e56da87533af4bf5f`，tree `79474ee7d251a54c07d4f0bba53b7b032a6f0420`。
- 独立实现提交：`278b59831aba82da19bb533763e86b68ac6c2a55`，**实际完整回归 / 新 wheel 认证 tree** `06dd1bba427d40849f3af4cab1a06c3fad6162e9`，已正常 push dev。
- main/local-origin-remote 保持 `edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`。最终独立证据提交只增 agents.md 与本附录/必要原始证据，认证后产品源/测试/metadata/工具未漂移。
- `requires-python` 从 `>=3.10,<3.14` 改为 **`>=3.10`**；取消人为上限、强制3.12策略，保持现有依赖钉版。3.10来自代码语法/NumPy2.2.6最低要求；28产品源码的3.10 AST语法检查通过，仅为语法核查，不冒称新3.10运行认证。
- README/PROJECT_RULES说明按最新稳定且实际兼容解释器选环境；`tests/test_p4_token_bootstrap.py` 与 `tools/verify_p2_revision.py` 子进程用 **sys.executable**，不再硬编码旧P4 `.venv312` 或3.12。token bridge仅改历史注释，不改凭证/网络门禁。
- 用户最新授权只允许确认项目venv的删除；不删除 target/数据/ledger/证据/未知目录/其他项目环境/全局Python/Rust。分批顺序获root确认：新keeper import/pip/token+bridge通过后删P4内8env，保留Fix3 py312/wheel312保底；完整回归成功后再删其余12env。

## 2. 最新稳定版不等于实际兼容

2026-09-30公开官方Python downloads及PyPI钉版metadata核查：最新稳定 **3.14.7**；3.15是预发布，不选。PyArrow18.1.0 / NumPy2.2.6 Windows x86-64 wheel有cp313、无cp314。

- `uv pip compile evidence/pins.in --python-version 3.14 --python-platform x86_64-pc-windows-msvc --only-binary :all:` **exit1**：当前pins无兼容wheel。
- 同命令 `--python-version 3.13` **exit0**。
- 实际 `uv venv --seed --python 3.13.15 .../python-env` 与正常 `uv pip install ... -e '.[dev,remote]' setuptools==75.6.0` **均exit0**。
- 未尝试盲升级Arrow/NumPy、移植包或用no-deps伪报安装；未尝试重型3.14源编译。结论限定为本机Windows及当前pins的已验证安装路线，不宣称3.14在所有平台绝不兼容。

**最终唯一保留：** `D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env`，CPython **3.13.15** 普通GIL版，base位于uv管理的全局解释器位置，base不删除。最终sakurapool为本仓库editable安装，调用该环境的 `Scripts/python.exe`（或先激活），不要用系统python代替。

| 依赖/工具 | 版本 |
|---|---|
| sakurapool | 0.1.0 |
| pyarrow / numpy / pyroaring | 18.1.0 / 2.2.6 / 1.1.0 |
| pytest / ruff / build | 8.3.4 / 0.9.2 / 1.2.2.post1 |
| requests / modelscope-hub | 2.32.5 / 0.4.0 |
| setuptools / pip | 75.6.0 / 26.2.1 |
| uv | 0.12.5 |

## 3. 本次实际验证，不借旧Fix3统计

简记 K=`D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`；全部worker验证显式绑定保留的 `.../rust-target/release/sakurapool-worker.exe`，PYTHONPATH unset。

| 命令/检查 | exit | 新结果 |
|---|---:|---|
| `K -I -c <dependencies+RustWorker import/profile>` | 0 | 原pins全import，实际模块来自仓库editable，解释器3.13.15 |
| `K -m pip check` | 0 | No broken requirements found |
| `K -m pytest tests/test_p4_token_bootstrap.py tests/test_r1_bridge.py tests/test_r1_stdout_backpressure.py tests/test_r1_worker_attempts.py -vv -rA --durations=10` | 0 | **62 passed，177.19s**，删前keeper基线 |
| `K -m pytest tests/ -vv -rA --durations=15` | 0 | **523 collected；521 passed、2 skipped，1386.04s**，正常pytest收集顺序；无排序插件、deselect/屏蔽失败/fixture变化 |
| `uv build --wheel --out-dir .../python-wheel` | 0 | 新metadata的纯Python wheel，非覆盖历史Fix3 wheel |
| 同keeper正常安装新wheel；仓库外 `K -I reports/R1-final/evidence/fix3_wheel_loop.py --worker ... --implementation 278b598... --tree 06dd1bba... --data-root D:/SakuraTool/SakuraPool-P4-work/python313-wheel-278b598` | 0 / 0 | noneditable site-packages、direct_url archive、HTTP scan→product adapter→P2/P3→Rust Range **PASS**；无需第二个venv |
| 同keeper恢复 `-e '.[dev,remote]'` | 0 | editable恢复原pins，当前模块来源仍为本仓库 |
| `K evidence/verify_final.py` | 0 | 唯一cfg环境；wheel28源码与implementation blob仅CRLF归一化后一致；Requires-Python>=3.10；523终态计数521/2；历史证据/现行源无漂移；pip check + ruff PASS |
| final helper内post-cleanup token/lifecycle两文件pytest | 0 | **36 passed，26.12s**，删除后仍可用，非重复全套 |

实际完整suite skips：
1. `tests/test_p4_package.py::test_package_fails_closed_on_invalid_inventory_or_binding[symlink]`；`tests/test_p4_package.py:179`：host lacks symlink privilege: OSError。
2. `tests/test_runtime_remediation.py::test_staging_audit_rejects_symlink_entry`；`tests/test_runtime_remediation.py:787`：this OS/user cannot create file symlinks。

新wheel闭环同合成10240B TAR/2成员，P2 samples1/errors0，P3 rid1，Rust gated Range bytes512-581共70B，服务器2GET，Range账attempts1/body70/inflight0/records1。复用未修改的Fix3 helper但传入新实现/tree，不复用历史结果。小raw fixture不是整个Pythonpipeline无全量缓冲证明；直接worker scan不冒称durable scan admission。uv纯Pythonbuild用了主机build解释器，安装/运行认证才是3.13.15；不将build成功算3.14 pinned runtime通过。

必要raw结果位于本目录evidence，保留真实失败：`resolve-python314.log` exit1为pin兼容拒绝；`candidate-dependencies.log` exit1因验证命令误写不存在的RustWorkerClient，纠正为实际RustWorker后import成功；两份postcleanup helper初次exit1分别为METADATA CRLF正则与pytest对齐空格解析错误，未修改产品/测试/旧log，修正helper后`python313-final.log`独立exit0。raw capture不覆盖已有log。未新增全文件/日志哈希清单；早期盘点/公开metadata日志中原有校验字段保留，不据此继续制造哈希。

## 4. 确认、删除路径与释放量

盘点范围仅仓库、P4-work、Fix3工具根、SakuraPool-p2-merge、P3-merge-a3a061-receipt；后二者无venv，不访问其他项目。首次确认20env；新建keeper后21；最终确认**1**。逐路径pyvenv.cfg与解释器版本记录、根/祖先/内部reparse检查、删除前CIM活动进程检查；删除批次所有目标均**非junction、非reparse、无活动进程/命令行引用**，不跟随链接。只删除下表精确venv根，保留父目录与其他内容。

| 批次 | 完整删除路径 | 逻辑文件bytes |
|---|---|---:|
| 1 | `D:/SakuraTool/SakuraPool-P4-work/.venv312` | 175153735 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/p4-cert-56f40c4/venv310` | 171824437 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/p4-cert-56f40c4/venv312` | 170767266 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/p4-cert-56f40c4/venv313` | 170725286 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/p4-cert-7f8afbc/venv312` | 132091 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/r1-final-py312` | 175413909 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/r1-final-wheel` | 140940343 |
| 1 | `D:/SakuraTool/SakuraPool-P4-work/r1-fresh-venv` | 163642187 |
| 2 | `D:/SakuraTool/SakuraPool/.venv` | 132054 |
| 2 | `D:/SakuraTool/SakuraPool/.venv310s` | 132961148 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312` | 177710461 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312benchrepair` | 168215302 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312finalr4` | 136802532 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312r4` | 136793551 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312w` | 141998883 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312w2` | 141972696 |
| 2 | `D:/SakuraTool/SakuraPool/.venv312w3` | 142141625 |
| 2 | `D:/SakuraTool/SakuraPool/.venv313s` | 132929300 |
| 2 | `D:/SakuraTool/SakuraPool-Fix3-20260930a/py312` | 175413894 |
| 2 | `D:/SakuraTool/SakuraPool-Fix3-20260930a/wheel312` | 174370877 |

- 逻辑文件总量 **2830041577B**；删除前GetCompressedFileSizeW+链接身份/nlink推算可释放文件extent合计 **2319703683B**，不含目录元数据；硬链接逻辑大小不等于物理释放。
- 批1卷free **473292427264→473974026240B**（+681598976B）；批2 **473851121664→475571675136B**（+1720553472B）。两个删除窗口观察free增量合计 **2402152448B**；是卷观测口径，包含目录/并发变化，不冒称精确因果释放或完整轮次净增量。
- 批1固定P4-root预算计量 **3102593024→1888645120B**，下降1213947904B；这只是获批venv文件移除，未改产品会计公式/额度/ledger。全suite新增10个twohop目录（329→339），增21012480B；删除批次逐个名称多重集/目录集合都保持不变，无历史offline-twohop清理。
- wheel合成产物保留后，固定根预算计量 **1911934976B**；4GiB cap4294967296B，stage1207959552B不变，当前margin **1175072768B**。不是预算提高/账本重置，也不是生产门禁解锁或永久headroom保证。

P4-work内 `.test-runner-v14r2.py`、`p4-run-cdn-v15.py`、`p4-run-redirect-host.py` 是已审历史一次性canary材料，按root确认保留原字节、不迁移路径、不运行；所引旧venv已删，不能直接运行，不算现行入口。未来真实验证需另获授权并使用当时验证过的runner，本轮不预建。

## 5. 文件生命周期、规则与边界

- 用户/root修改的既有untracked `agents.md` 本轮明确授权显式纳入最终文档提交；不连带其他untracked。全局规则文件/接线由root处理，本写入方不改全局设置、不以其回执冒称本轮独立验证。
- MEMORY仅保留唯一keeper、兼容原因、历史env路径已退役与报告入口，受.gitignore忽略、不提交。一次性任务plan/公开metadata抓取脚本及自产evidence __pycache__在确认无后续用途后回收；保留必要raw验证、删除审计及其可审查源、新wheel和合成数据。
- 历史Fix3报告/43证据文件原字节保持；旧py312/wheel312的实际删除不改写当时通过的认证。新521/2及新wheel是本次变更证据，旧认证不自动覆盖新契约。
- 保留旧/新Rust target、所有数据/ledger/未知缓存/用户文件；既有dirty P3 ruff及其他untracked不纳入。本轮不做main合并、生产数据/ModelScope请求、R2/P5、强推、reset/rebase/amend或全根清理。
- 所有性能为合成回归/闭环耗时，非生产吞吐。数据下载服务未访问；官方包源/解释器下载和git同步属于授权开发网络操作。

结论：`PYTHON_COMPATIBILITY PASS`、`UNIQUE_PROJECT_VENV PASS`、`PY313_FULL_REGRESSION PASS`、`PY313_WHEEL PASS`；R1历史完成不改为P4完成，`P4_COMPLETE NO`、`R2/MERGE/P5 NO`。正常提交/push/核验后 **STOP / WAITING_REVIEW**。
