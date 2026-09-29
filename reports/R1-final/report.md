# R1 收尾 10 门 · Gate J 最终报告（v2，completion 修订版）

基线提交：`398c31a3c1b4fbb57b9e0420a777873823776f48`（Gate A–H，`origin/dev` 已核验一致）。
本报告随 completion 修订提交入库；该提交 SHA 见交付回复与 `git log -1 -- reports/R1-final/report.md`。

## 1. 范围

完成 R1 收尾计划 Gate A–J：单 crate 布局、正式 NDJSON 协议 v1 + Python 持久预算、Rust `scan_tar` 顺序扫描器、离线端到端闭环、协议集成测试、生命周期/崩溃恢复、环境闸门、最终全量验证、本报告。全部工作仅在 `dev`；全部离线合成数据。

## 2. 中间提交（旧→新）

| SHA | 门 | 说明 |
|---|---|---|
| `a3ae457c4f6f69cd85fba2b29980b88016550d4d` | A | 单 crate `rust/`（`sakurapool-rust`），钉 1.98.1 windows-gnu，`--locked` 构建 |
| `55da5dbab66018c66fa6f04773776264d26a5ca6` | B+C | NDJSON 协议 v1（hello/ready、capabilities、有界 stderr、cancel-kill+wait）；`BudgetLedger` 唯一持久预算权威，Rust `JobBudget` 纯内存、admit 即耗 attempt |
| `09c8ce4427391de6640d026b5ac7e1cda795ddb5` | D | `scan_tar`：tar crate 顺序扫描，整包+逐成员 sha/offset/size；拒链接/稀疏/设备/FIFO/绝对路径/`..`/非 UTF-8/截断/非零尾；`max_members`/`max_bytes` 限额 |
| `502427d3fc5e0f334e5efe0d6509a02e83e08fb4` | E | 离线 e2e：raw → Rust scan → Python audit（按 Rust 偏移重哈希原字节，不重解析 TAR）→ staged 契约 → `write_staged_v4` 真实 P2 durable v4 → P3 compile/query → Rust 预算门控 range fetch，字节级一致 |
| `0a16594e0089a6c6747e4a8a33dcbb91376fd270` | F | 协议集成补齐：hung worker 触发 bridge 接收超时（崩溃类、租约 pending）；真实管道 stderr 溢出 drain（保最后 64 KiB、无死锁） |
| `af6c8910bde127c177b0ecdc80162525e3e60c09` | G | Python 进程硬崩溃（`os._exit`）恢复：durable 租约原样保留不退款，新进程同 root 可见且账本可用 |
| `398c31a3c1b4fbb57b9e0420a777873823776f48` | H | 环境闸门：`tests/conftest.py::resolve_r1_worker` 唯一解析入口 + 会话头恒报告；无二进制时 R1 全 skip 且 exit 0；offline 静态证明；fixture 限于仓库外固定 P4 工作根 |
| （completion 修订提交） | J-v2 | 变量契约修复 + 3.12 正式证据 + 本报告 |

## 3. completion 修订（本交付）

### 3.1 变量契约修复（单 P 为唯一契约）

worker 环境变量统一为 **`SAKURAPOOL_RUST_WORKER`**（单 P）。改动仅 2 文件共 6 处：
`tests/conftest.py:4,30,46`（文档串、解析器、会话头指引）与 `tests/test_r1_env_gate.py:29,34,47`（无 worker 演练与解析器单测）。
**不保留双 P 兼容**；生产代码不变（bridge 只接受显式二进制路径、无 env 回退）。

漂移溯源（字节级 `git show | od -c` 验证）：
1. `91d0048`（P4 期）`src/.../rust_bridge.py::_WORKER_ENV = "SAKURAPPOOL_R1_WORKER"`（双 P 旧名）；
2. `a3ae457`（Gate A）src 解析器 `"SAKURAPPOOL_RUST_WORKER"`（双 P）；
3. `55da5db`（B+C）测试层 `"SAKURAPOOL_RUST_WORKER"`（单 P，src 解析器同期移除）；
4. `398c31a`（Gate H）conftest 静默改回双 P —— **未声明的契约漂移，v1 报告呈报并确认**；本次按裁决修复为单 P，双 P 拼写自 P4 旧名继承的品牌错误一并消除。

### 3.2 正式 3.12 证据（.venv312，Python 3.12.13）

解释器：`D:/SakuraTool/SakuraPool/.venv312/Scripts/python.exe`（`--version` → Python 3.12.13）。
按授权仅在该 venv 安装缺失项目依赖：`requests==2.32.5`（`[remote]` 钉版）、`ruff==0.9.2`（`[dev]` 钉版）。
既有钉版依赖在场：pyarrow 18.1.0、numpy 2.2.6、pyroaring 1.1.0、packaging 26.3、pytest 9.1.1。
口径披露：`[dev]` 钉 pytest==8.3.4，venv 实有 9.1.1，未降级（安装范围仅限缺失项）；`[remote]` 的 modelscope-hub 未被 R1 四文件引用，未安装。

### 3.3 旧 3.14 结果降级

v1 报告中全部系统 Python **3.14.5**（`C:\Users\PC\AppData\Local\Python\pythoncore-3.14-64\python.exe`）运行——全量回归 497 passed/2 skipped（2515.18 s）、R1 专项 32 passed、无 worker 演练——现降级为**诊断**：3.14.5 超出 `pyproject.toml` `requires-python = ">=3.10,<3.14"` 声明，非正式解释器（沿用 P4 既有约定的产物）。§4 的 3.12 结果为正式证据。Rust 侧结果与 Python 版本无关，维持正式。

## 4. 命令 / 退出码 / 统计

### 4.1 正式 3.12（本交付执行）

| 命令（`.venv312/Scripts/python.exe`，`PYTHONPATH=src`，`SAKURAPOOL_RUST_WORKER=…/rust-target/release/sakurapool-worker.exe`） | exit | 结果 |
|---|---|---|
| `-m pytest tests/test_r1_bridge.py tests/test_r1_loopback_loop.py tests/test_r1_gate_e_e2e.py tests/test_r1_env_gate.py -q -p no:cacheprovider` | 0 | **32 passed, 0 failed**，146.55 s |
| `-m ruff check .`（全项目） | 0 | All checks passed |

注：R1 专项中的无 worker 演练（`test_r1_env_gate.py::test_r1_suite_skips_and_reports_without_worker`）以子进程方式在 **3.12 venv 解释器**内复跑 `test_r1_bridge.py`，其中已含单 P 变量的权威性验证。

### 4.2 正式 Rust（不变，`CARGO_TARGET_DIR=D:\SakuraTool\SakuraPool-P4-work\rust-target`，工具链 1.98.1-x86_64-pc-windows-gnu）

| 命令 | exit | 结果 |
|---|---|---|
| `cargo fmt --check` | 0 | 无 diff |
| `cargo clippy --all-targets --locked -- -D warnings` | 0 | 0 warning |
| `cargo test --locked` | 0 | lib 4 + `tar_scan` 7 + `worker_protocol` 12 = **23 passed / 0 failed** |
| `cargo build --release --locked` | 0 | 无重编译（字节未变） |

### 4.3 诊断 3.14（降级，历史执行）

| 命令 | exit | 结果 |
|---|---|---|
| 全量 `python -m pytest tests/ -q`（系统 3.14.5） | 0 | 497 passed, 2 skipped, 0 failed，2515.18 s |
| R1 四文件（系统 3.14.5） | 0 | 32 passed |
| `ruff check .`（系统 3.14.5） | 0 | All checks passed |

## 5. 专项证据摘要（与 v1 一致，测试代码除 §3.1 外未变）

- **E**：`tests/test_r1_gate_e_e2e.py::test_rust_scan_drives_p2_p3_rust_fetch`（236 行，`502427d` 新增，`git diff 502427d HEAD -- <file>` 为空）：`:165` Rust `scan_tar` → `:171-190` 按 Rust 偏移重哈希原字节 audit → `:194` `_build_stage`（仅用 Rust 结果）→ `:196` `open_completed_stage` → `:198` `write_staged_v4`（`samples==2, errors==0`）→ `:207` `compile_runtime`（`rid_count==2`）→ `:211` tag 查询 → `:221` `fetch_range_gated`（P3 精确范围）→ `:230` `raw[off:off+size] == PNG_1X1` 字节级一致。
- **F**：hung worker → `worker timed out`，账本 `attempts==1, body==4096, records==0`，`cancel()` 后 `pid is None`；stderr 100 KiB → tail 恰 65 536 B、drain 无死锁。
- **G**：子进程 reserve 后 `os._exit(0)` → 新进程同 root `attempts==1, body==3000, inflight≥3000`；后续 consume+settle 后 `body==3100, attempts==2`。
- **H**：静态扫描 R1 测试源码 URL host ⊆ {`127.0.0.1`, `example.invalid`}；服务器绑定均 127.0.0.1；`DEFAULT_WORK_ROOT` 在仓库外。

## 6. 性能口径

仅合成小数据（≤128 KiB 图像、≤200 KiB 载荷），非生产性能证据。3.12 正式 R1 四文件 146.55 s；全量 41:55（3.14 诊断）较 P4 基线 1415 s 的增量与 `big32_corpus` 缓存重建行为一致（未逐测试计时验证，仅作口径说明）。

## 7. 日志字节数及哈希

未将运行日志落盘为文件；上表为逐命令实际终端输出（verbatim 摘要行）。worker 二进制 SHA-256：`c1aab9e18d8cb107f14b35161cb015c325ed54c6bd5bad7aa7aeadb1aa5865b3`；`rust/Cargo.lock` SHA-256：`329f3c72315fab90cd10f8390283f2fad8001581cd1d6afa7733c946667a1824`。

## 8. 限制与安全声明

- 全程离线：仅 loopback TCP、仓库外固定工作根合成 fixture；未访问生产数据/数据服务，未启动大规模任务；无真实 TAR、无 R2/P5、未触 `main`。
- worker 未接入生产 transport；生产门禁维持 BLOCKED。
- 3.12 正式证据覆盖 R1 四专项 + 全项目 `ruff check`；含 P4 的全量套件仍在 3.14.5 诊断口径（venv312 系列缺 `requests` 前不能跑 P4 全套的 P4 约定；本次已装 `requests`，全量是否升为正式证据由后续执行单决定）。
- `.venv312` 的 pytest 为 9.1.1（dev 钉 8.3.4），已如实披露，未降级。
- 工作区既有 dirty `reports/P3/raw/ruff.log` 未触碰；未使用 `git add -A`，仅显式暂存本轮文件。
