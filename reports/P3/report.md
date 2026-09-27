# P3 运行时返修交付报告（reviewer A–J 全项）

日期：2026-07-09（会话时间）
状态：返修完成，待审查（WAITING_REVIEW）

## 1. 范围与提交

- 权威 BASE / main / origin main：`6c45548e01b0856d6d4bbce18e240be0b22ae7e7`
  （P2 最终提交；此前报告误写为 P1 的 `52358d6f`，已按 root 指示更正）。
- dev 分支 P3 实现提交（完整 SHA，origin 可核）：
  1. `8aaa2998330d74ca576c0e56a1629f499e23c83a` feat(runtime): add P3 compiler, snapshot, query and CLI
  2. `d1852ea105c7e876bc5bb75d9c82831a1806b995` test(runtime): add synthetic P2 generator and determinism/crash coverage
  3. `6ee1841315c5f871445b86f62a0c828b6967b2d0` test(runtime): add EXPLAIN index proof and random differential
  4. `ea65bf27acc3fac450e86f9c0da30552c5e4d27e` feat(runtime): add P3 benchmark tool and document the runtime（被本次 CHANGES_REQUIRED 审查）
  5. `27939cbfb74a6b68ef9fb0252dd4e07cc6c111d7` fix(p3): reviewer A-H remediation
  6. `9d05e02c8434ebac4676f56566135accfedcbde9` fix(p3): benchmark measurement integrity (item I) + 32 MiB eviction evidence (item D)
  7. `9e62fd6b3b54eaebb906b9e5d484b7f1df316e13` fix(p3): correct open_s/locations100k measurement, refresh P3 docs
  8. 本报告的 report 提交（SHA 见推送后 `origin/dev`）。
- 分支状态：dev 本地与 origin/dev 同步（本提交后 push 并核验远端 SHA）；main 未动；无 PR。

## 2. 逐项验收（A–J）

| 项 | 修复 | 实际测试（tests/test_runtime_remediation.py 及既有套件） |
| --- | --- | --- |
| A §37/38 | `locations.npy` 追加 82 字节身份 trailer（`SAP3LOC1`+snapshot_id+rid_count），fast open 绑定整文件身份；同 shape 整文件置换 → `SnapshotMixError`；`bitmaps.sqlite`/`catalog.sqlite` 整文件置换 → 元身份失配拒绝；显式 snapshot 目录 open 修复（目录名↔manifest id 绑定，改目录名即拒）；`snapshot.query(sources=...)` 关键字入口；`full_verify` 改流式 hash | `test_locations_payload_tamper_only_full_verify`（payload 同 size 原位篡改 fast open 不检、full 检出，验收边界按 root 约定）；`test_whole_file_swap_same_shape_rejected`（locations 整置换/截断/bitmaps 整置换）；`test_explicit_snapshot_dir_open`；`test_keyword_query_entry_and_len_and_object_ref` |
| B §34/48-50 | READY 复用改为验证：READY 内容、manifest、每个发布文件流式 sha256（损坏即 fail-closed，不静默复用）；`current.json` 损坏/丢失在下次 compile 修复；外部 `.staging-<other-id>` 目录不删除（所有权要求）；STAGE 断点复用逐产物校验（既有 resume 验证保留并扩展） | `test_ready_reuse_validated_and_current_repaired`（READY 内容损坏拒绝、payload 位翻转拒绝、current.json 重建）；`test_staging_ownership_respected`（外部 staging 用户文件保留）；既有 crash/resume 全套（每个 stage 边界注入崩溃） |
| C §10/57 | 新增 `_sha256_file` 流式 hash（1 MiB 分块），替代 catalog/locations 整文件 `read_bytes`；fast open 只读 npy magic 6B + trailer 82B；`full_verify` 全文件流式 | `test_streaming_hash_parity`（0/1/1KiB/1MiB+12345 与 hashlib 全等）；5M 基准 query 进程 peak RSS 152 MiB（修复前 795 MiB 量级的整文件读入已消除） |
| D §25/59 | ByteLRU：单 blob 超预算不缓存（reviewer 实测 limit=4 put 9B 现在 resident=0）；负/零预算抛 ValueError；默认 256 MiB 保持 | `test_byte_lru_oversized_and_budget`（limit4 put9 resident0 miss）；`test_byte_lru_32mib_real_eviction`（32 MiB 实际驱逐 48 次 1 MiB blob，resident 恒 ≤ 预算）；`test_default_cache_budget_is_256mib` |
| E §28/29/30 | 裸未知 namespace 必抛（含 any_of 分支内）；`QueryResult.__len__`；`object_ref(object_idx)` 返回完整 ObjectRef（§15 公开入口）；record batch 增加 `source_name`/`dataset_name`（§15 名称输出）；planner 按 catalog 存储基数排序（不先 load），AND 交为空即停止物化后续 bitmap；多 source/dataset term 正确 OR | `test_bare_unknown_namespace_errors`；`test_keyword_query_entry_and_len_and_object_ref`；`test_record_batches_carry_names`；`test_planner_short_circuits_on_stored_cardinality`（基数序断言 + 第三项永不物化） |
| F §39/§7 | CLI 增加 `--index/--snapshot/--spec/--full`（含 verify/inspect/lookup 别名）与 `--spec` JSON `any_of` 入口；不再宣称“无 query CLI”；fingerprint 本就排除 `COMMIT.created_at`（inventory 既有实现正确），**报告错误已更正**，并新增确定性测试钉住该契约 | `test_cli_spec_and_flags`（--snapshot/--full/--spec any_of JSON/--index 四种形态 + --spec 与 term 混用拒绝）；`test_created_at_does_not_change_snapshot_id`（只改 created_at → snapshot_id 不变） |
| G §5 | inventory 对任何未声明文件 fail-closed（orphan `.partial` 拒绝）；校验 `object_id == rel@sha256(input)`；fragment 行级 (dataset, object) 归属校验（列投影流式，不整表载入）；synthetic_p2 改为真实 P2 契约：BLAKE2b-16 RecordKey、object/version/validator = input sha256、ObjectRef 构造合法、零长 metadata 存在性、uint64>2**63 选项；先小真实 fixture 闭环再跑规模 | `test_inventory_rejects_stray_files`（orphan.partial / 未声明 parquet 均拒）；`test_inventory_rejects_row_ownership_violation`（行 object_id 篡拒绝）；`test_synthetic_p2_follows_real_contract`（BLAKE2b16 record id 断言 + ObjectRef 合法 + 真实 indexer fixture 编译查询闭环）；100k/1M/5M 全部基于新契约生成 |
| H §13/14 | uint64 无损性：SQLite 存储路径用 BLOB/TEXT 安全编码（既有 `_u64` 校验 + 结构体 memmap 直接 uint64 落盘），`>2**63` offsets/size 合法且不分配大文件；metadata presence 独立布尔（零长 metadata 仍 HAS_METADATA） | `test_uint64_offsets_and_metadata_presence`（2**63 偏移回读无损、json_size=0 时 flags=1、has_json=False 时 flags=0、object_size=2**64-1 合法、越界拒绝） |
| I | 基准：e2e 计时（query→first128 单跨度为 gate，plan/extract 分开报告）；10k 恰好 10000 行不超；cold 每 family 独立 snapshot+cache；每 phase JSON 绑定代码+options 指纹（旧 tree 缓存必重跑，不冒充最终 tree）；100k locations 证据 | 最终 tree 全量重跑（run_id 见 benchmark.json）：5M gate 全 PASS；100k diff 0/300；`locations100k` rows=100000 |
| J | pinned 3.12 venv（uv，正常 wheel 安装非 --no-deps）+ `pip check` + CLI 验证；reports/P3 全字节 hash 提交 | `.venv312`（Python 3.12.13，pyarrow 18.1.0/numpy 2.2.6/pyroaring 1.1.0 与 P2 pins 一致）：pip check 无破坏；CLI compile/query/--spec 全 rc=0；**154/154 测试在 3.12 全通过**（tests-3.12.log） |

测试总数：154（host Python 3.14.5 与 pinned 3.12.13 双跑均 154/154），ruff 全清。
原 102 测试独立复跑通过保持（本次未改 pins）。

## 3. 命令 / 退出码 / 统计

| 命令 | 退出码 | 统计 |
| --- | --- | --- |
| `python -m pytest -q`（host 3.14.5，PYTHONPATH=src） | 0 | 154 passed（16.3 s） |
| `.venv312/Scripts/python -m pytest -q -p no:cacheprovider` | 0 | 154 passed（16.5 s，tests-3.12.log） |
| `python -m ruff check .` | 0 | All checks passed |
| `.venv312/Scripts/python -m pip check` | 0 | No broken requirements found |
| `python tools/bench_runtime.py --workdir build/bench-final` | 0 | 全 gate PASS（raw/bench-final.log） |
| CLI e2e（3.12 venv：compile→query→--spec/--full） | 0/0/0 | count 正确（raw/venv312.txt） |

## 4. 基准（最终 tree，run 指纹绑定）

- compile：100k 2.3 s / 1M 51.2 s / 5M 315.0 s；peak RSS 147 / 211 / 372 MiB
  （5M/1M 增长 1.76× < 5× gate）；peak temp 39 / 395 / 1991 MiB。
- 5M gate：warm_count_p95_200ms=True（实测 max p95 1.09 ms）、
  first128_e2e_p95_500ms=True（实测 max e2e p95 0.62 ms，e2e=query→first128
  单跨度）、ten_k_1s=True（max p95 10.2 ms）、locations100k_reported=True
  （100000 行 7 ms）、cache_resident_bounded=True。
- 100k 差分：0/300 mismatch（Python-set 参照 vs bitmap）。
- 合成数据口径：全部为小型合成语料的诊断数字，不作生产性能推断。

## 5. 报告更正（此前自述错误）

1. fingerprint 含 `created_at`：错误。inventory 实现本就排除
   `COMMIT.created_at`；本次新增 `test_created_at_does_not_change_snapshot_id`
   钉住该行为，不改代码。
2. “无 query CLI”：错误。`cli.py` 早有 `runtime query`；本次补齐
   `--index/--snapshot/--spec/--full` 与 JSON any_of 入口。
3. BASE 误报 P1 `52358d6f`：更正为 `6c45548e01b0856d6d4bbce18e240be0b22ae7e7`。
4. 5M peak RSS 795 MiB：那是旧代码（整文件 read_bytes hash）的数字；最终
   tree 流式化后为 372 MiB（见 §4）。

## 6. 限制与安全声明

- 未访问生产数据/数据服务，未启动授权外的大规模任务；全部测试与基准使用
  小型合成数据（5M 规模仅为内存/时间 gate 的诊断基准，非生产性能）。
- 未合并 main、未建 PR、未删远端分支、未强推；未 amend 已送审提交
  （`ea65bf2` 保持原样，其缺陷由后续提交修复）。
- root 旧文件（agents.md、todo.md、根目录 diff/tar 等）未触碰。
- 本目录所有文件字节哈希见 `hashes.sha256`；报告文件自身不包含自身哈希。
