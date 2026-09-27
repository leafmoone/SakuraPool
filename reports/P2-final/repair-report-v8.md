# P2 Pending Cleanup Repair Report v8

## 身份与边界

- BASE: `52358d6fca728d2bba12814490e0974a6907b218`
- PREVIOUS_SUBMISSION: `d27ef9ba66fc874ccfa4f33b91a9f8ff5ea35802`
- NEW_IMPLEMENTATION: `c496a5cac0f8725b71fb6894e58f13bbd59f8f23`
- NEW_SUBMISSION: 本报告提交后的新 SHA
- main: `52358d6fca728d2bba12814490e0974a6907b218`，未修改
- dev: implementation 后追加本报告/evidence commit

本轮仅修复 P2 pending cleanup 生命周期。storage profile、AVIF、FORMAT_VERSION=4、builder v4、strong SHA、streaming 等已通过契约未扩展。未 amend、reset、rebase、merge，未启动 P3/P4，未访问 ModelScope 或生产数据。root `.gitignore`、`agents.md`、`MEMORY.md`（如存在）以及 `SakuraPool-P2.diff`、`SakuraPool-P2-9ec0519.tar.gz` 未修改、未 stage。

## CleanupHandle 与可操作重试

每个 scan/shard 使用私有 `_SpoolScope`，scope 创建受控 `CleanupHandle`。当清理失败时，业务异常仍保持原类型，但附带 `cleanup_handle` 属性；handle 不依赖 traceback 或局部变量，独立持有 scope 和 pending resources。

`cleanup_handle.pending_resources` 返回每个待释放 resource 的完整绝对 path 与该 resource 的 errors。`cleanup_handle.retry()` 只重试该 scope 的 owned resources，并返回新的 pending 列表。重试成功后 scope resources 和 pending_resources 都为空；普通成功 cleanup 也会 discard resource。

本轮真实隔离测试覆盖：

- 成功业务 `SpoolCleanupError` 离开异常块后提取 handle，`gc.collect()`，恢复 unlink fault，`handle.retry()` 后 SQLite/sidecars 实际消失。
- reference `RuntimeError` 主异常同样携带 handle；离开异常块并 gc 后恢复 fault，retry 成功，primary 类型和 notes 不依赖局部 traceback。
- 多资源 partial cleanup：一项 PermissionError 时另一项成功释放并从 scope 移除，失败项保留完整 path/errors；retry 后 pending/resources 为空。
- 外部预建 scope 不受 scan failure 清理，外部 SQLite 仍可 `SELECT 1`，文件仍存在。

### Resource 释放语义

`_SpoolState` 持有 path、原始 `NamedTemporaryFile` handle 和 sqlite connection。创建文件后立即进入 scope；handle close、connect、DDL、commit 全部受保护。release 尽力关闭 db、关闭 handle、删除自身 SQLite 及自身 `-journal/-wal/-shm`。只有所有步骤无错误才清空状态并 discard；失败 resource 保留，可通过 handle retry 重试。用户文件不在 scope 中。

### Marker 时序

实际代码和测试时序为：

1. `_write_fragments` 写完、关闭、fsync 并校验所有 Parquet。
2. `_write_fragments` rename 全部 Parquet 文件并完成对应 checkpoint。
3. `_write_fragments` 返回后，scan 的 `finally` 关闭 rows spool。
4. rows cleanup 成功后重新验证 input，再写 COMMIT marker。

成功路径 rows unlink fault 因此会传播 `SpoolCleanupError`，不会进入 marker 写入；测试断言 output 无 COMMIT。这个证据明确只覆盖 fault 发生在上述 marker 发布前的情形，不假称 marker 已发布后的清理状态。

失败路径中，reference/业务 primary 先被显式保存；rows cleanup failure 通过 `add_note` 记录真实 cleanup error，primary 原类型保留，并附带同一 cleanup handle。

## 独立 fault matrix

最终专项回归通过：

1. 首次 connect failure
2. 第二次 connect failure
3. DDL failure
4. constructor commit failure
5. close failure
6. handle close 抛错且未实际关闭，随后 retry
7. 成功路径 unlink failure 阻止 COMMIT
8. reference primary + unlink failure notes
9. partial multi-resource cleanup 与 retry
10. 外部 scope 保护
11. scan `SpoolCleanupError` gc 后 handle retry
12. reference `RuntimeError` gc 后 handle retry

这些测试使用真实隔离 tempdir、SQLite 文件/sidecar 快照和用户文件字节断言，不依赖全局临时目录扫描或 traceback 生命周期。

## 最终验证

验证针对 `NEW_IMPLEMENTATION` tree 执行，完整 verifier exit 0：

- host pytest：exit 0，`tests=100 failures=0 errors=0`
- pinned Python 3.12/PyArrow 18.1 pytest：exit 0，`tests=100 failures=0 errors=0`
- host/pinned ruff：exit 0
- worktree diff check：exit 0
- 完整 BASE..HEAD diff check：exit 0
- wheel build、clean install、wheel import isolation、pip check：均 exit 0
- installed CLI version/config/scan：均 exit 0
- fixture E2E、extent probe、crash/resume：均 exit 0
- cleanup handle/scope/marker fault matrix：专项通过

`reports/P2-final/manifest.json` 记录最终命令 argv、exit code、输出 bytes、SHA256；JUnit XML 为最终 host/pinned 测试树机器输出。v6/v7 报告保留历史，不作为本轮 cleanup 顺序或证据。

最终 synthetic local TAR benchmark（10000 pairs，包含整 TAR SHA，排除 fixture generation）：

- samples: 10000
- errors: 0
- total_seconds: 1.388230000040494
- header_seconds: 0.40837449999526143
- json_seconds: 0.07121750374790281
- parquet_seconds: 0.1643897999310866
- peak_rss_bytes: 84115456

这是 synthetic local evidence，不是生产或 ModelScope 性能声明。Pinned clean install 使用 PyArrow 18.1 并通过完整测试。

## 终止状态

报告/evidence commit 后停止在 `WAITING_REVIEW`。无合并批准、无 P3/P4、无 main 变更、无 ModelScope 访问、无生产数据访问。
