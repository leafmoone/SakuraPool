# P4 Storage/Profile Repair Report v5

## 身份与授权边界

- BASE: `52358d6fca728d2bba12814490e0974a6907b218`
- PREVIOUS_SUBMISSION: `9ec051928d9b6c871febe6b1e4bd1bc8e2790acb`
- NEW_IMPLEMENTATION: `75d8ce6c7837aafd9c8eff2b5b2c3639a00859e9`
- NEW_SUBMISSION: 本报告提交后的新 SHA
- main: `52358d6fca728d2bba12814490e0974a6907b218`，未修改
- dev: implementation 后追加本报告/evidence commit
- 中间 implementation SHA：`75d8ce6c7837aafd9c8eff2b5b2c3639a00859e9`

本轮撤销 `9ec051` 的可合并结论，仅在 `dev` 追加修复。未 amend、reset、rebase、merge，未启动 P3，未访问 ModelScope 或生产数据。根 `.gitignore`、`agents.md`、`MEMORY.md`（如存在）以及 `SakuraPool-P2.diff`、`SakuraPool-P2-9ec0519.tar.gz` 均保留，未修改、未 stage。

## 最终契约

- `FORMAT_VERSION = 4`
- `BUILDER = sakurapool-p2-v4`
- 默认 `storage_id = local`；adapter 可显式配置稳定 profile，例如 `modelscope-main`。
- `storage_id` 是存储 profile，不等同于 content-bound `object_id`。同一 profile 的两个 TAR 验证为相同 storage_id、不同 object_id。
- `ObjectRef` 字段为 `storage_id/object_id/object_path/object_size/object_version/validator`，附带 backend、repo_type、validator metadata；`MemberRef` 对 uint64 offset/size 和 object_size bounds 做校验。
- `repo_type` 不再表示 TAR；当前 local-specific 值为 `local`，也允许 nullable。`archive_format=tar` 单独表达物理归档格式。
- v3 输出与 v4 不兼容，旧 P2 output 不可复用；必须输出到新目录，不做隐式迁移。
- 默认 image extensions 为 `.jpg/.jpeg/.png/.webp/.avif`；不解码图像。AVIF 配对回归验证 format=avif、indexed seek 读取范围和原 payload 完全一致。
- README 与常量 machine assertion 同步 v4/builder，避免版本漂移。
- README 保留唯一限制语句：`P4 MUST NOT use remote full-object SHA rereads as the normal ModelScope validation path.` 固定 revision/object validator 是后续方向。本轮仍只做本地 TAR，不实现远端 provider。

## 资源生命周期

`_MemberSpool` 和 `_SpoolRows` 注册自身 SQLite 临时文件；close 会关闭连接、删除文件并从活动注册表移除。scan 的 shard wrapper 在任意异常路径执行 cleanup，包括 metadata 异常、identity conflict、Parquet 写入/校验失败、reference validation、checkpoint 失败和其他异常。failure-injection 回归验证 scan 前后 spool 集合为空，并验证用户自己的临时文件保留。

TarFile 逐 member 使用 `TarFile.next()`，随后立即清空 Python 3.12 `TarFile.members`。2051-member 回归的峰值保留 TarInfo 数为 1。成员数据只保留 SQLite 标量 extents，不保留完整 TarInfo 集合。

## 验证与性能证据

最终验证针对 NEW_IMPLEMENTATION tree 执行，完整 verifier exit 0。全部 gate：

- host pytest：exit 0，`tests=88 failures=0 errors=0`
- pinned Python 3.12 pytest/PyArrow 18.1：exit 0，`tests=88 failures=0 errors=0`
- host/pinned ruff：exit 0
- 完整 BASE..HEAD diff check：exit 0
- wheel build、clean install、wheel import isolation、pip check：均 exit 0
- installed CLI version/config/scan smoke：均 exit 0
- fixture E2E、extent probe、crash/resume：均 exit 0
- storage/profile、ObjectRef roundtrip/bounds、AVIF payload、spool failure、v4 README assertion：专项 5/5 通过

最终 synthetic benchmark（10000 pairs、单 TAR、包含整 TAR SHA、排除 fixture generation）：

- samples: 10000
- errors: 0
- total_seconds: 1.7238588000182062
- header_seconds: 0.47927560005337
- json_seconds: 0.08897900197189301
- parquet_seconds: 0.23887760005891323
- peak_rss_bytes: 84340736
- Python 3.14.5 / PyArrow 25.0.1 / Windows 11

该数据仅是小型 synthetic evidence，不是生产性能或 ModelScope 性能宣传。Pinned clean install 使用 PyArrow 18.1，且完整测试通过。

## 证据文件

- `reports/P2-final/manifest.json`：每个命令 argv、exit code、输出 bytes、SHA256
- `reports/P2-final/host-junit.xml`
- `reports/P2-final/pinned-junit.xml`
- `reports/P2-final/benchmark.log`
- `reports/P2-final/extents.log`
- `reports/P2-final/repair-summary-v5.json`
- `reports/P2-final/evidence-manifest-v5.json`

日志 hash 以 manifest 和 evidence manifest 中的机器生成 SHA256 为准；证据 manifest 使用最终 Git index blob bytes 计算。历史 v3/v4 报告仅为历史记录，不伪造为本轮证据。

## 终止状态

报告提交后停止在 `WAITING_REVIEW`。没有合并批准，没有 P3，没有 main 变更，没有 ModelScope 访问，也没有生产数据访问。
