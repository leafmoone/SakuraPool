# P2 ObjectRef Archive Format Repair Report v9

## 身份、范围与状态

- BASE: `52358d6fca728d2bba12814490e0974a6907b218`
- PREVIOUS_SUBMISSION: `2f13b0d2153347644c8d7342546580d0fb1579ce`
- NEW_IMPLEMENTATION: `44b30f36f844285d03f298c9cdc6664437717e43`
- NEW_SUBMISSION: 本报告提交后的新 SHA
- main: `52358d6fca728d2bba12814490e0974a6907b218`，未修改
- dev: implementation 后追加本报告/evidence commit
- status: `WAITING_REVIEW`

本轮唯一任务是 P2 `ObjectRef.archive_format` 修复。FORMAT_VERSION 仍为 4，BUILDER 仍为 `sakurapool-p2-v4`；storage profile、AVIF、strong SHA、streaming、pending cleanup 等既有语义不扩展、不回退。未 amend、reset、rebase、merge，未启动 P3/P4，未访问 ModelScope 或生产数据。root `.gitignore`、`agents.md`、`MEMORY.md`（如存在）及旧 `SakuraPool-P2.diff`、`SakuraPool-P2-9ec0519.tar.gz` 未修改、未 stage。

## ObjectRef 最终语义

`records.ObjectRef` 是 self-contained retrieval object descriptor，字段完整为：

```text
storage_id
object_id
object_path
object_size
object_version
validator
backend
repo_type
archive_format
validator_kind
validator_strength
```

`archive_format` 必须是非空 canonical string：无首尾空格、无 whitespace、使用小写 canonical form。P2 local 对象固定 `repo_type=local`、`archive_format=tar`。`repo_type=tar` 被显式拒绝；本轮没有加入 zip、压缩格式或 backend 扩展。对象 Arrow schema 已有 `archive_format`，本轮保持 FORMAT_VERSION=4 和 builder v4，不产生 schema bump。

README 已说明 ObjectRef 是 self-contained retrieval descriptor，P2 local 使用 `repo_type=local` / `archive_format=tar`，远程 revision retrieval 尚未实现，并保留原句：

`P4 MUST NOT use remote full-object SHA rereads as the normal ModelScope validation path.`

## 新增专项证据

新增并实际运行四项专项，结果 `4 passed`：

1. `archive_format=tar` roundtrip，空值和非 canonical 值拒绝，`repo_type=tar` 拒绝。
2. 从真实 `objects.parquet` 行无损构造完整 ObjectRef，断言 `storage_id/repo_type/archive_format == local/local/tar`。
3. 对 image 和 JSON 两类 MemberRef 从 objects/sample Parquet 回读 `offset_data/size`，对原 TAR 做 indexed seek，payload 与原始 bytes 完全相等。
4. 同一 storage profile 的两个 TAR 保持相同 stable storage_id、不同 content-bound object_id，且 archive_format 均为 tar；既有 AVIF payload 专项仍通过。

## 最终验证命令与统计

最终验证在 `NEW_IMPLEMENTATION` tree 上执行，完整 `verify_p2_revision.py` exit 0：

- host pytest：exit 0；JUnit `tests=102 failures=0 errors=0`
- pinned Python 3.12 / PyArrow 18.1 pytest：exit 0；JUnit `tests=102 failures=0 errors=0`
- host ruff：exit 0
- pinned ruff：exit 0
- worktree diff check：exit 0
- 完整 BASE..HEAD diff check：exit 0
- wheel build：exit 0
- clean wheel install：exit 0
- installed wheel import isolation：exit 0
- `pip check`：exit 0
- installed CLI version/config/scan：exit 0
- fixture E2E、extent probe、crash/resume：exit 0
- 新增 ObjectRef/archive_format/storage/MemberRef 专项：`4 passed`

最终 benchmark（synthetic local TAR，10000 pairs，包含整 TAR SHA，排除 fixture generation）：

- samples: 10000
- errors: 0
- total_seconds: 1.5289890000130981
- header_seconds: 0.4298747999127954
- json_seconds: 0.07875250291544944
- parquet_seconds: 0.2044273999053985
- peak_rss_bytes: 84914176

该 benchmark 只代表 synthetic local evidence，不是生产或 ModelScope 性能声明。Pinned clean install 使用 PyArrow 18.1，并通过完整测试。

## 增量 diff 证据

用户指定增量文件此前不存在，因此本轮创建，没有覆盖未知文件：

```text
SakuraPool-P2-final-repair.diff
range: 2f13b0d2153347644c8d7342546580d0fb1579ce..44b30f36f844285d03f298c9cdc6664437717e43
bytes: 7257
sha256: d1a1a725ba31d35d7563f4ce6d9c3fff5d342f8fea0b16b6cdafee2e57b77b9c
```

报告/evidence 以及最终 manifest 记录命令输出 bytes/hash；最终 evidence manifest 使用 Git index blob bytes 计算。历史 v5/v6/v7/v8 报告保留历史标识，不冒充本轮 ObjectRef archive_format 验证或旧日志。

## 终止声明

报告/evidence commit 后停止在 `WAITING_REVIEW`。没有合并批准，没有 P3/P4，没有 main 变更，没有 ModelScope 访问，没有生产数据访问。
