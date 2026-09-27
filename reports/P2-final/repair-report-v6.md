# P2 Spool Cleanup Repair Report v6

## 身份与边界

- BASE: `52358d6fca728d2bba12814490e0974a6907b218`
- PREVIOUS_SUBMISSION: `573f28cbe8a5b1e6c39b472de69d660bf29c078d`
- NEW_IMPLEMENTATION: `6491aafb6d032eaec697ae5c7596d619e534556a`
- NEW_SUBMISSION: 本报告提交后的新 SHA
- main: `52358d6fca728d2bba12814490e0974a6907b218`，未修改
- dev: implementation 后追加本报告/evidence commit

本轮仅修复 spool ownership/cleanup 及 README P2/identity 文案。已通过的 storage profile、AVIF、FORMAT_VERSION=4、builder v4、ObjectRef、strong SHA 等契约未扩展。未 amend、reset、rebase、merge，未启动 P3/P4，未访问 ModelScope 或生产数据。根 `.gitignore`、`agents.md`、`MEMORY.md`（如存在）以及 `SakuraPool-P2.diff`、`SakuraPool-P2-9ec0519.tar.gz` 未修改、未 stage。

## 实现变更

### 构造 ownership

`_MemberSpool` 和 `_SpoolRows` 在 `NamedTemporaryFile` 创建并关闭后立即登记自身路径，登记值初始为 `None`；随后才执行 sqlite connect、DDL 和构造 commit。connect、DDL、commit 任一失败都会通过统一 release 路径关闭已有 connection，删除自身 SQLite 主文件及自身 `-journal`、`-wal`、`-shm` 文件，并在 cleanup 失败时给 primary exception 添加 note。成功构造后 connection 才被登记为 owner value。

### close 与异常保留

`close()` 不再先 commit 暂存数据。它直接尽最大努力 close、删除主文件和 SQLite sidecar 文件；任何清理失败都会显式抛出 `SpoolCleanupError`，不能伪称已删除。scan 的 cleanup 遍历全部 owned resources，即使一项失败仍继续其他项，并把 cleanup errors 作为 note 附加到 primary exception。用户路径不在 ownership registry 中，因此不会被删除。

rows spool 在 reference validation、Parquet 写入/核验、checkpoint 及其他 scan 异常时由外层 `finally` close；shard wrapper 对 scan 内 primary exception 执行全局剩余 owned resource cleanup。报告只声明下述实际注入路径得到的证据，不把活动 registry 为空单独当成无泄漏证明。

## 独立 failure-injection 证据

真实隔离 tempdir 测试共 8 项通过：

1. 第一次 sqlite connect 注入失败：owned SQLite 无残留。
2. DDL 注入失败：owned SQLite 无残留。
3. 构造 commit 注入失败：owned SQLite 无残留。
4. 第二个 spool connect 注入失败：第一个成功 spool 与第二个半构造 spool 均释放。
5. close 自身失败注入：主文件释放、`SpoolCleanupError` 明确报告 cleanup failure。
6. primary reference/runtime error 与 cleanup OSError 组合：primary exception 保留，cleanup failure 作为 note。
7. 每条隔离测试检查 `-journal/-wal/-shm` sidecar 不残留。
8. 用户自有 `user.sqlite`/临时文件字节保持不变。

这组测试使用独立 owned tempdir 和真实文件快照；不依赖 `_ACTIVE_SPOOLS == {}` 作为唯一证据。已有 checkpoint/crash-resume 回归仍保留。

README 首行已恢复 P2；对象语义已改为 stable storage profile 加 content-bound `object_id`，并明确 required missing metadata 保留 sample。未来警告保留：

`P4 MUST NOT use remote full-object SHA rereads as the normal ModelScope validation path.`

## 最终验证

最终验证针对 `NEW_IMPLEMENTATION` tree 执行，完整 verifier exit 0：

- host pytest：exit 0，`tests=93 failures=0 errors=0`
- pinned Python 3.12/PyArrow 18.1 pytest：exit 0，`tests=93 failures=0 errors=0`
- host/pinned ruff：exit 0
- worktree diff check：exit 0
- 完整 BASE..HEAD diff check：exit 0
- wheel build、clean install、wheel import isolation、pip check：均 exit 0
- installed CLI version/config/scan：均 exit 0
- fixture E2E、extent probe、crash/resume：均 exit 0
- cleanup/storage/AVIF/FORMAT4 专项：8 项 cleanup injection 与 v4/storage/AVIF 专项均通过

机器证据位于 `reports/P2-final/manifest.json`，包含每个命令 argv、exit code、输出字节数和 SHA256。JUnit XML 是最终 host/pinned 测试树的机器输出。

最终 synthetic benchmark（10000 pairs、单本地 TAR、包含整 TAR SHA、排除 fixture generation）：

- samples: 10000
- errors: 0
- total_seconds: 1.4925546000013128
- header_seconds: 0.4263358999742195
- json_seconds: 0.07792070449795574
- parquet_seconds: 0.1744708000915125
- peak_rss_bytes: 84373504
- Python 3.14.5 / PyArrow 25.0.1 / Windows 11

这是 synthetic local evidence，不是生产或 ModelScope 性能声明。Pinned clean install 使用 PyArrow 18.1 并通过完整测试。

## 证据与终止

本轮最终文件：

- `reports/P2-final/repair-report-v6.md`
- `reports/P2-final/repair-summary-v6.json`
- `reports/P2-final/evidence-manifest-v6.json`
- `reports/P2-final/manifest.json`
- `reports/P2-final/host-junit.xml`
- `reports/P2-final/pinned-junit.xml`
- `reports/P2-final/benchmark.log`

报告/evidence commit 后停止在 `WAITING_REVIEW`。没有合并批准、P3/P4、main 变更、ModelScope 访问或生产数据访问。
