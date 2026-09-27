# P2 Scoped Spool Cleanup Repair Report v7

## 身份与范围

- BASE: `52358d6fca728d2bba12814490e0974a6907b218`
- PREVIOUS_SUBMISSION: `41c2ab0febf65a41055fd0a475eff65b89c29fee`
- NEW_IMPLEMENTATION: `0fbade467cc808ebf9db4f6d16efafec491bb0f1`
- NEW_SUBMISSION: 本报告提交后的新 SHA
- main: `52358d6fca728d2bba12814490e0974a6907b218`，未修改
- dev: implementation 后追加本报告/evidence commit

本轮限定为 P2 cleanup 修复。未修改 storage/AVIF/FORMAT4/builder/strongSHA 契约；未 amend、reset、rebase、merge，未启动 P3/P4，未访问 ModelScope 或生产数据。root `.gitignore`、`agents.md`、`MEMORY.md`（如存在）及 `SakuraPool-P2.diff`、`SakuraPool-P2-9ec0519.tar.gz` 未修改、未 stage。

## 具体修复

### Primary exception 与成功路径 cleanup

scan 现在在进入 `finally` 前显式保存 `primary`：正常路径为 `None`，reference/Parquet/checkpoint 等失败路径为实际异常对象。清理失败时：

- `primary is None`：cleanup error 直接传播为 `SpoolCleanupError`，scan 不继续到 marker 发布。
- `primary is not None`：primary 原样保留，真实 cleanup error 通过 `add_note` 附加。

不再在 `finally` 内用 `sys.exc_info()` 推断当前异常。

发布时序回归注入 `_remove_spool_files` unlink failure，验证 scan 返回 `SpoolCleanupError` 且 output 没有 `COMMIT` marker。这个证据明确对应 failure 发生在 row spool close、Parquet publication 和 marker 写入之前；本报告不把它泛化为 marker 已发布后的行为，也不声称未测试路径的提交状态。

reference primary + rows unlink failure 回归验证 `RuntimeError("reference primary")` 保留，异常 notes 包含真实 `unlink injection`。

### Handle 和资源所有权

新增 `_SpoolState` 保存 path、原始 NamedTemporaryFile handle 和 sqlite connection。创建成功后立即加入当前 `_SpoolScope`，不再在登记前调用 handle.close。handle close、connect、DDL、commit 全部处于构造保护范围。

`_release_spool` 逐项尽力关闭 db、关闭原始 handle、删除自身 SQLite 主文件和自身 `-journal/-wal/-shm`。只有所有这些步骤都没有异常，resource 才清空 db/handle 状态并从 scope 注销；任何失败都保留 pending resource，使调用者可以重试。实际“handle close 抛错且未实际关闭”回归验证首次 cleanup 保留资源和文件，解除 fault 后 retry 成功删除并确认原始 handle closed。

### Scope 隔离

删除跨 scan 的全局 active-resource cleanup。每次 `_scan_shard` 创建私有 `_SpoolScope`，MemberSpool 和 RowsSpool 只加入该 scope；失败只清理该 scope 的资源。预建在另一个 scope 的外部 RowsSpool 在 scan 失败后仍可执行 `SELECT 1`，自身文件仍存在，随后独立 close。

这避免了一个 scan 错误清理另一个 scan 或调用者拥有的 spool。cleanup 证据不依赖全局 registry 为空。

## 独立 fault matrix

最终 cleanup 专项为 8/8：

1. 首次 connect failure
2. 第二次 connect failure
3. DDL failure
4. constructor commit failure
5. close failure
6. handle close failure（真实未 close，之后 retry release）
7. 成功 scan unlink failure（`SpoolCleanupError`，marker 尚未发布）
8. reference primary + unlink failure（primary 保留、note 正确）

另有 scope isolation 回归：外部预建 spool 不受 scan failure 清理，SQLite 查询和文件均保持可用。所有测试使用隔离 tempdir；用户 sqlite 字节保持不变，SQLite sidecar 也纳入快照检查。

## 最终验证

验证针对 `NEW_IMPLEMENTATION` tree 执行，完整 verifier exit 0：

- host pytest：exit 0，`tests=97 failures=0 errors=0`
- pinned Python 3.12/PyArrow 18.1 pytest：exit 0，`tests=97 failures=0 errors=0`
- host/pinned ruff：exit 0
- worktree diff check：exit 0
- 完整 BASE..HEAD diff check：exit 0
- wheel build、clean install、wheel import isolation、pip check：均 exit 0
- installed CLI version/config/scan：均 exit 0
- fixture E2E、extent probe、crash/resume：均 exit 0
- cleanup fault matrix：8/8

`reports/P2-final/manifest.json` 记录每条命令的 argv、exit code、输出 bytes 和 SHA256；JUnit XML 为最终 host/pinned tree 的机器输出。旧 v5/v6 报告保留历史，不作为本轮 cleanup 结论或日志证据。

最终 synthetic local TAR benchmark（10000 pairs，包含整 TAR SHA，排除 fixture generation）：

- samples: 10000
- errors: 0
- total_seconds: 1.439748500008136
- header_seconds: 0.41167679999489337
- json_seconds: 0.07819929963443428
- parquet_seconds: 0.15799139998853207
- peak_rss_bytes: 84611072

这是 synthetic local evidence，不是生产或 ModelScope 性能声明。Pinned clean install 使用 PyArrow 18.1 并通过完整测试。

## 终止状态

报告/evidence commit 后停止在 `WAITING_REVIEW`。无合并批准、无 P3/P4、无 main 变更、无 ModelScope 访问、无生产数据访问。
