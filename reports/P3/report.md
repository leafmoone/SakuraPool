# P3 运行时返修交付报告（reviewer 第一轮 A–J + 第二轮 7 项）

日期：2026-07-09（会话时间）
状态：两轮返修完成，待审查（WAITING_REVIEW）

## 1. 范围与提交事实

- 权威 BASE / main / origin main：`6c45548e01b0856d6d4bbce18e240be0b22ae7e7`
  （P2 最终提交）。
- dev 分支 P3 实现提交（完整 SHA，origin 可核；第二轮已补第一轮遗漏的 9d85c4）：
  1. `9d85c4bf570e5ae8e480850d29f9dbc105e1ce28` feat(runtime): add P3 v1 base and strict P2 inventory
  2. `8aaa2998330d74ca576c0e56a1629f499e23c83a` feat(runtime): add P3 compiler, snapshot, query and CLI
  3. `d1852ea105c7e876bc5bb75d9c82831a1806b995` test(runtime): add synthetic P2 generator and determinism/crash coverage
  4. `6ee1841315c5f871445b86f62a0c828b6967b2d0` test(runtime): add EXPLAIN index proof and random differential
  5. `ea65bf27acc3fac450e86f9c0da30552c5e4d27e` feat(runtime): add P3 benchmark tool and document the runtime（两轮审查基线提交，未 amend）
  6. `27939cbfb74a6b68ef9fb0252dd4e07cc6c111d7` fix(p3): reviewer A-H remediation
  7. `9d05e02c8434ebac4676f56566135accfedcbde9` fix(p3): benchmark measurement integrity (I) + 32 MiB eviction evidence (D)
  8. `9e62fd6b3b54eaebb906b9e5d484b7f1df316e13` fix(p3): correct open_s/locations100k measurement, refresh P3 docs
  9. `addaa7c86b2c17bdaa7cb1129669375431b75b0d` docs(p3): 第一轮报告（本轮重建其内容）
  10. `27799c5b20e4839a8c06f8717c90c468f300e307` fix(p3): ownership protocol, source/dataset planner cardinality, real-Roaring eviction, env-bound bench fingerprint
  11. `7c2f220e1c9cb5b96e3352059556b0c748e11aaf` feat(p3): record full env and run fingerprint in the benchmark artifact
  12. 本报告提交（SHA 见推送后 `origin/dev`）。
- 分支状态：每提交即 push 并核验远端 SHA；main 与 origin/main 未动；无 PR、
  无强推、无远端删除、无 amend。

## 2. 第一轮 A–J（摘要；第一轮明细见 addaa7c 版报告，测试名不变）

A 身份 trailer + 置换拒绝 + 显式 snapshot open + keyword query；
B READY 验证复用 + current.json 修复 + staging 不删外部目录；
C 流式 hash（fast open 只读 6B+82B）；D ByteLRU 超预算不缓存 + 32 MiB 实际驱逐；
E 裸未知 namespace、`__len__`、object_ref、record batch 名称、存储基数规划；
F CLI `--index/--snapshot/--spec/--full` + JSON any_of；created_at 报告错误更正（代码本就正确）；
G inventory 全目录 fail-closed + 行级归属 + synthetic 真实 P2 契约（BLAKE2b-16）；
H uint64 BLOB 无损 + 零长 metadata presence 独立布尔；
I e2e 计时 / 10k 上限 / 每 family 独立 cold / 代码+options 指纹缓存；
J pinned 3.12 + reports/P3 提交。

## 3. 第二轮 7 项逐项验收

**R2-1 staging/final 数据删除缺陷（紧急）** — `27799c5`
- 根因：原实现仅凭目录名 `.staging-<snap_id>` 判归属；发布目录还自带
  staging 的旧 marker 语义，无法区分“发布目录”与“中断发布”。
- 修复：显式 ownership 协议 —— `OWNER.json`（protocol=1、owner=
  runtime-compiler、snapshot_id、**role: staging/published**、known_files
  清单）+ **已知文件审计**（目录内任何文件超出编译器文件清单即视为外部）：
  - 预建 `.staging-<正确 snap_id>/user.txt`（无 marker）→ compile 抛
    `SnapshotCorruptError`，**user.txt 保留**（reviewer 原样复现已覆盖）；
  - 伪造 marker（含 role）但目录含外部文件 → 审计仍 fail-closed；
  - 发布分支删除 `snapshots/<snap_id>` 前要求 marker role=staging + 已知文件
    审计全过；发布完成时 marker 翻转为 role=published，已发布目录永不满足
    删除条件（rename 与翻转间崩溃留下的 READY 目录由验证复用路径先行
    返回/拒绝，到不了删除分支）。
- 实际测试：`test_same_id_staging_without_marker_fail_closed`（reviewer
  复现 + 手动清理后可重编译）、`test_same_id_staging_forced_marker_fail_closed`、
  `test_unowned_snapshot_dir_never_deleted`（含 marker 但含外部文件仍拒删）、
  `test_staging_ownership_respected`（异 id staging 保留）、既有 crash/resume
  全套（marker 由编译器自身写入，恢复语义不变）。

**R2-2 hashes.sha256 按提交 blob 重建** — 本报告提交
- 根因：上一版按 Windows worktree 字节（CRLF）计算，与提交 blob（LF）不符。
- 修复：全部 artifact 先 stage，哈希一律对 **index/blob 字节**
  （`git cat-file blob :path`）计算，提交后再按实际 blob 复核（见 §5 核验步骤）。

**R2-3 真实 wheel 认证（非 editable）** — 证据 `raw/`
- `uv build --wheel` 产出 `dist/sakurapool-0.1.0-py3-none-any.whl`（真实
  build，非 editable）；全新 clean venv `.venv312w`（Python 3.12.13）以
  **wheel + 正常依赖解析**安装（非 --no-deps、非 editable）。
- repo 外（`C:/Users/PC/AppData/Local/Temp/sp3-wheel-verify`）、清除
  PYTHONPATH 运行：`pip check` 无破坏；assert `sakurapool.__file__` 位于
  site-packages 且 sys.path 无 repo src；CLI compile / query /
  query --snapshot / query --spec(any_of) / verify --full / inspect / lookup
  全部 rc=0（`raw/venv312-cli-verify.log` 完整原始输出，含命令/cwd/exit）。
- pinned 全测试对 **wheel 安装**跑通：159/159（`tests-wheel-3.12.log`）。

**R2-4 32 MiB/真实 Roaring 驱逐** — `27799c5`
- 新增 `test_real_runtime_eviction_correctness`：120000 样本 4 标签、真实
  RuntimeSnapshot 查询触发 **真实 Roaring blob 加载驱逐**（3 个 16.4 KB
  blob 于 48 KiB 预算），驱逐前后结果逐一相同，hit/miss/eviction/resident
  全记录且 honest（含 Belady 轮换陷阱的规避说明）。既有 ByteLRU 32 MiB
  字节级测试保留。

**R2-5 source/dataset planner stored=0** — `27799c5`
- `_stored_cardinality` 泛化：source/dataset 单 id 取 bitmaps 表真实
  cardinality，OR 组取存储和上界（load 前），不再硬编码 0。
- 实际测试：`test_planner_source_dataset_cardinality_order`：rare tag
  （cardinality 1）先于 source/dataset（101）加载，且**全 AND 条件集**
  （source+dataset+all+any+none）下 rare 仍最先、计数精确。

**R2-6 reports/P3 结构** — 本报告提交
- 新增 `evidence-manifest.json`（每命令：command/cwd/exit/bytes/sha256）、
  `query-benchmark.json`（query 阶段 + gates 抽取）、完整原始日志
  （bench、CLI、双跑测试、ruff、pinned env 摘要），不再以 8 行摘要代替
  CLI 日志；implementation 列表补齐 `9d85c4bf…`。

**R2-7 基准指纹覆盖 env + 实际重跑** — `27799c5` + `7c2f220`
- run 指纹覆盖：**整个 src 树**（含 records/indexer 等全部 P2 契约模块）+
  bench 工具 + 生成器 + **Python 版本 + numpy/pyarrow/pyroaring 版本** +
  options；换认证环境/改任何源码，旧缓存 phase 自动失效。
- 报告 artifact 记录 `env` 与 `run_fingerprint`（自证依据）。
- 最终 tree + 认证 3.12 wheel 环境 **实际重跑 100k/1M/5M**（§4 数字即
  重跑结果，非旧缓存）。

## 4. 命令 / 退出码 / 统计（最终 tree `7c2f220`，全部原始日志在 raw/）

| 命令（cwd: 仓库根，除非注明） | 退出码 | 统计 |
| --- | --- | --- |
| `env -u PYTHONPATH .venv312w/Scripts/python -m pytest tests -q -p no:cacheprovider` | 0 | 159 passed（28.2 s，wheel 安装） |
| `PYTHONPATH=src python -m pytest tests -q -p no:cacheprovider` | 0 | 159 passed（23.7 s，host 3.14.5） |
| `python -m ruff check .` | 0 | All checks passed |
| `.venv312w/Scripts/python -m pip check`（cwd: repo 外 temp） | 0 | No broken requirements found |
| repo 外 CLI 7 条（compile/query×3/verify --full/inspect/lookup） | 0×7 | 见 raw/venv312-cli-verify.log |
| `env -u PYTHONPATH .venv312w/Scripts/python tools/bench_runtime.py --workdir build/bench-final` | 0 | 无 PERFORMANCE_GATE_FAIL |

## 5. 基准（最终 tree 重跑，认证 3.12 wheel 环境，run 指纹绑定）

- env：Python 3.12.13 / Windows-11 / numpy 2.2.6 / pyarrow 18.1.0 /
  pyroaring 1.1.0；run_fingerprint 见 benchmark.json（全 src+env 绑定）。
- compile：100k **5.2 s** / 1M **82.4 s** / 5M **466.0 s**；peak RSS
  **159 / 252 / 449 MiB**（5M/1M 增长 **1.79×** < 5× gate）；peak temp
  **39 / 395 / 1991 MiB**。
- 5M gate 全 PASS：warm_count_p95_200ms、first128_e2e_p95_500ms（e2e 实测
  max p95 0.93 ms）、ten_k_1s（max p95 9.75 ms）、locations100k_reported
  （100000 行 / 8 ms）、cache_resident_bounded。
- 100k 差分：**0 mismatch**。query 进程 peak RSS 53 / 73 / 153 MiB。
- 与第一轮数字差异说明：本轮在 pinned 3.12 wheel 环境重跑（第一轮在 host
  3.14），compile 时间/RSS 随解释器与 tree 变化属正常；口径均为小型合成
  语料的诊断数字，不作生产性能推断。

## 6. 报告更正（累计）

1. fingerprint 含 created_at → 错误（代码正确，已有测试钉住）。
2. “无 query CLI” → 错误（CLI 已存在并补齐兼容参数）。
3. BASE 误报 P1 `52358d6f` → 更正为 `6c45548e`。
4. 第一轮实现列表漏 `9d85c4bf…`（P3 v1 base）→ 已补（§1 第 1 条）。
5. 第一轮 hashes.sha256 按 worktree CRLF 计算与 blob 不符 → 本版按
   index/blob 字节重建（§3 R2-2）。
6. 第一轮 5M 数字（315 s / 372 MiB）为 host 3.14 口径；本轮 3.12 wheel
   环境为 466 s / 449 MiB，报告以重跑值为准。

## 7. 限制与安全声明

- 未访问生产数据/数据服务/授权外大规模任务；测试与基准全部小型合成数据
  （5M 仅为内存/时间 gate 的诊断基准）。
- 未合并 main、未建 PR、未强推、未删远端分支、未 amend 已送审提交；
  root 旧文件（agents.md、todo.md、根目录 diff/tar 等）未触碰。
- `_u64` 8 字节定长 BE BLOB 编码经 reviewer 独立复核通过，本轮未重复改动。
- 本目录所有文件字节哈希见 `hashes.sha256`（对提交 blob 计算）；报告文件
  自身不包含自身哈希；`evidence-manifest.json` 逐命令给出 sha256 与字节数。
