# SakuraPool P4 阶段报告 — P4_R1 · WAITING_REVIEW

## P4-R1 阶段执行记录

- **固定基线**：`MAIN_BASE=edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`；`R1_START=5b10f7e153fc7094a32e4e27cf41eba9c90f0418`；执行分支 `dev`。Preflight 实际核验：本地 HEAD=`R1_START`，`origin/dev`=`R1_START`，`origin/main`=`MAIN_BASE`。
- **保护边界**：未修改 `main`，未 amend/reset/rebase/force-push，未执行 ModelScope 或其它真实仓库请求，未执行完整真实 TAR 扫描、上传、R2/P5；既有 dirty/untracked 文件保持原样，未使用 `git add -A`。

### R1 工具链安装（用户授权，系统/用户标准位置，不在仓库内）

- `winget install --id Rustlang.Rustup --exact`（v1.29.1，exit 0）；rustup home `C:\Users\PC\.rustup`，cargo bin `C:\Users\PC\.cargo\bin`。rustup-init 标准行为把 `C:\Users\PC\.cargo\bin` 写入**用户级注册表 PATH**（已核验）；未改机器 PATH，未写入仓库任何位置。
- 构建 target 放仓库外：用户级环境变量 `CARGO_TARGET_DIR=D:\SakuraTool\SakuraPool-P4-work\rust-target`（已持久化）；`rust/target` 不存在。
- 链接器事实：系统无 MSVC 构建工具，`link.exe` 解析为 Git 自带 POSIX `C:\Program Files\Git\usr\bin\link.exe`，MSVC 链接全部失败（`link: extra operand`）。改用官方替代：`rustup toolchain install stable-x86_64-pc-windows-gnu` + MSYS2 ucrt64 MinGW（`winget install MSYS2.MSYS2` + `pacman -S mingw-w64-ucrt-x86_64-gcc`，均 exit 0），默认 toolchain 设为 `stable-x86_64-pc-windows-gnu`。
- **新进程核验**（PowerShell 从注册表 Machine+User PATH 构建干净环境块后启动进程）：`where.exe rustc` → `C:\Users\PC\.cargo\bin\rustc.exe`（exit 0）；`where.exe cargo` → `C:\Users\PC\.cargo\bin\cargo.exe`（exit 0）；`rustc --version` → `rustc 1.98.1 (48a229cea 2026-09-01)`（exit 0）；`cargo --version` → `cargo 1.98.1 (797e8a9bc 2026-08-05)`（exit 0）；`rustup show` → active `stable-x86_64-pc-windows-gnu`（exit 0）；`rustup target list --installed` → `x86_64-pc-windows-msvc`、`x86_64-pc-windows-gnu`。组件：rustc/cargo/rust-std/rust-docs/rustfmt/clippy 齐全。

### R1 Rust core 阶段（`rust/` workspace，仅离线原语）

- 新增：`rust/Cargo.toml`（workspace）、`rust/crates/sakurapool-r1/`（lib + `sakurapool-r1-worker` NDJSON 二进制 + 集成测试）。lib 提供：持久化有限预算账本（reserve 先持久化、settle 只减差额、崩溃不自动退款、重复预留/超额拒绝）、精确 Range/Content-Range 校验、流式 SHA-256、response lifecycle（created/begin/complete/cancel）。worker 为 stdin NDJSON 请求 → stdout NDJSON 应答的进程级协议，含 cancel。
- 修正记录：worker 首版 `reserve` 存在类型/语义 bug（新条目 id 错误复用末条），已改为直接写入调用方 id；`ByteRange` 补 `is_empty` 消除 clippy 警告。
- 实际验证（`CARGO_TARGET_DIR` 指向仓库外，全部真实执行）：
  - `cargo fmt --check`：exit 0（先 `cargo fmt` 修复格式差异，复验通过）。
  - `cargo test`：exit 0；`test result: ok. 4 passed`（lib 单元）+ `ok. 1 passed`（worker NDJSON 进程级集成）。
  - `cargo clippy --all-targets --all-features`：exit 0，0 warning。
  - `cargo build --release`：exit 0。
- 哈希：`rust/Cargo.lock` SHA-256 `4af42f2c4de975cd9379dd43f1d64ad5c1d6f24af61e957928991c7cfda8e2d8`；release 二进制 `D:\SakuraTool\SakuraPool-P4-work\rust-target\release\sakurapool-r1-worker.exe`（576,493 B）SHA-256 `cb501474cde7eb14a1415dfe1afccb4e93dec659260c99f32983665156794888`。

### R1 当前缺口（未完成，不伪报）

- Python 薄 bridge 调用 Rust worker 的 NDJSON 集成尚未实现；Rust 侧流式 SHA-256/Range 校验尚未接入 Python transport 生命周期。
- 离线 loopback 全链闭环（scan→P2 durable v4→P3 compile/query→Rust fetch 字节一致）未开始；预算迁移（请求前持久预留/块内内存计数/崩溃不退款覆盖输出/临时/IPC）未做；4 GiB 预算语义改应用数据工作集口径的 docs 迁移未完成。
- wheel/fresh-process R1 来源验证、Python 全量回归在 R1 收尾阶段执行。
- **阶段结论**：`P4_R1=WAITING_REVIEW`，`P4_COMPLETE=NO`，`MERGE_AUTHORIZED=NO`，`R2/P5=NO`。

> 本文件第 1–6 节保留历史 P4 game dataset 认证口径，历史实现冻结为 `56f40c43d0a85943b633c38eab6a4175d53b434f`。当前 repository 运行时配置化实现已在 dev 新提交 `dfd5121af933ac5f63b66ed031ce46a569dcf469` 完成；当前 `konachan_full` 目标验证见下方“当前配置化 addendum”，不覆盖旧 game 证据。

## 当前配置化 addendum（2026-09-29）

- **实现提交/tree**：`dfd5121af933ac5f63b66ed031ce46a569dcf469` (`feat(p4): make repository runtime configurable`)，tree `64e7db4b11c92903e9777a7592d9b0156ae49ec6`；未 amend、未 main。P4 repository 现在是运行时 `owner/name` 字符串，由 `location_gate.parse_repository` 唯一解析；`ModelScopeDataset`、tree/probe/proof、`BoundObject` 和 legacy condition proof 全链绑定同一配置 repository。P2 package manifest 的冻结 `_REPO` 仍是另一契约，未宣称支持 package schema 迁移。
- **测试**：受影响套件 `216 passed, 1 skipped`，ruff 全部通过。当前提交后的工作树无 P4 源码/测试未提交改动；工作树剩余 P2/P3 报告噪音未纳入。
- **目标**：`leafmoone/konachan_full`。token 直接来自 `D:/sm_data/ms-token.tmp`，token 值未写入证据。
- **真实网络（初次错误路径）**：目标为 `leafmoone/konachan_full`，`network_request_started=true`。OpenAPI endpoint HTTP **200**，tree endpoint HTTP **200**；初次 legacy `/repo` Range `bytes=0-0` 错误复用了旧 `game_cg_5M` 路径 `pre/gamecg-v1-pre-p02-003.tar`，返回 HTTP **404**。该 404 不能作为公开仓库不可访问的证据。
- **真实网络（按 SakuraMoon 正确路径）**：tree 返回的实际 TAR 为 `images/0000.tar`，size `912343040`。同一 `Revision=master`、`FilePath=images/0000.tar` 的 origin `/repo` 返回 HTTP **302**；按公开 SakuraMoon 的两跳方式，对该次响应的 CDN `Location` 不携带凭证发送 `Range: bytes=0-0`，返回 HTTP **206**，`Content-Range: bytes 0-0/912343040`、`Content-Length: 1`，实际读取 1 B。1B Range 已验证；没有完整下载、TAR 扫描、生产解锁或 If-Match 验证，产品仍未解锁。
- **目标证据**（仓库外固定工作根，未覆盖旧 game 证据）：
  - `D:/SakuraTool/SakuraPool-P4-work/reports/P4/konachan_full-real-verification.json` — 1507 B，SHA-256 `5ececb7bf8595bee43714bd81d3e9e21f517a88461b76090b64a3ce2c716b51d`。
  - `konachan_full-real-verification.stdout.log` — 1219 B，SHA-256 `7f52de97757f458bf3778d4cb9ffcdc9cb1827c1312cb7ed487482f80c0e6f67`。
  - `konachan_full-real-verification.stderr.log` — 0 B，SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。
  - `konachan_full-ledger-after.json` — 207 B，SHA-256 `610c9ba8e6b4858cda9e4ba352abc71f443568e09e8eda0d60d58bd28d2f5372`。本次没有 before 快照，不能伪造 ledger delta。
  - `konachan_full-correct-path-range.json` — 正确 tree 路径及两跳 Range 证据，SHA-256 `9bdc2671599deecc15c5680abdaa9e2173470fdbb4c477c7cabcabd6e8f4e221`。
- **账本与当前结论**：本次只保存了 `konachan_full-ledger-after.json`，**ledger-before 缺失，因此不能计算或声称 before/after delta**。配置化实现已提交；公开仓库 OpenAPI/tree 与按真实 tree 路径执行的 1B 两跳 Range 均成功。P4 仍为 `P4_PARTIAL / WAITING_REVIEW`：产品未解锁，完整下载、TAR 扫描、生产解锁和 If-Match 仍未执行，不能升级为 `P4_IMPLEMENTED`。

## game_cg_5M 真实验证 addendum

- **实现绑定**：当前实现 `dfd5121af933ac5f63b66ed031ce46a569dcf469`，实现 tree `64e7db4b11c92903e9777a7592d9b0156ae49ec6`。本 addendum 只补充已完成的 `leafmoone/game_cg_5M` 只读验证，不修改旧 game 或 konachan 证据。
- **真实事实**：token 来自 `D:/sm_data/ms-token.tmp`，值未记录；OpenAPI HTTP **200**；tree HTTP **200**，`TotalCount=147`；真实 tree 路径 `pre/gamecg-v1-pre-p00-001.tar`，声明大小 `2240399360` B；origin `/repo` HTTP **302**；随后对同次 CDN `Location` 以**无凭证**方式请求，CDN HTTP **206**；`Range: bytes=0-0`，`Content-Range: bytes 0-0/2240399360`，`Content-Length: 1`，实际读取 **1 B**。
- **目标证据**（仓库外固定工作根，均为脱敏文件）：
  - `D:/SakuraTool/SakuraPool-P4-work/reports/P4/game_cg_5M-real-verification.json` — 1192 B，SHA-256 `87f23a7bf7676ca67854700e779a97b3ca35fb704d1cc99d2d4e30f48f286911`。
  - `game_cg_5M-ledger-before.json` — 186 B，SHA-256 `c5c6525c9bbbd3d611f5dcf68f16062f1882283fd4fb73fef446b914aa7a97b7`；状态为 `not_collected`，不是伪造快照。
  - `game_cg_5M-ledger-after.json` — 215 B，SHA-256 `0e92fd7ba299269b3d77f194960307492ef7bf718f141174ca1ef222a7f8e448`；状态为 `not_collected`，因此 delta 为 `not_computable`。
  - `game_cg_5M-verification.stdout.log` — 238 B，SHA-256 `a98a5c345b333f6bbb7cc48a968cf8ee5f0325f72343bf2ae551feb552312f0e`。
  - `game_cg_5M-verification.stderr.log` — 0 B，SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。
- **明确未完成/未执行**：未执行完整 TAR 下载、一次顺序 TAR scan、P2 durable v4、P3 compile、package build、production fetch、并发 `1/4/8` 矩阵、fresh-process 验证；也未证明固定工作根的 4 GiB 物理边界。没有绕过生产门禁，没有上传/修改远端数据。P4 仍为 `P4_PARTIAL / WAITING_REVIEW`，不能据此升级为 `P4_IMPLEMENTED`。

## 1. 基线、提交、边界

- BASE、`main`、`origin/main`：`edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`。两笔连续实现提交：`7f8afbcb5185a348fc069fb339308eef2a77c1b1`（guarded remote discovery、预算、离线读写 seam）；`56f40c43d0a85943b633c38eab6a4175d53b434f`（production fetch 禁止与可核 durable package 纠正）。均在 `dev` 正常 push；`git ls-remote origin refs/heads/dev refs/heads/main` 于后笔实现提交后核验 `origin/dev=56f40c43d0a85943b633c38eab6a4175d53b434f`、`origin/main=BASE`。尚无报告提交；报告-only 提交会另列，**不回写被认证产品 SHA**。
- 实现提交仅显式暂存授权源码/测试；工作树原有 `reports/P3/raw/ruff.log` 变化与未知附件未暂存、未清理。没有 amend/reset/rebase/force push、main 合并、P5 或**向 ModelScope 目标上传数据/发布索引**；正常 Git `origin/dev` 代码推送已发生。`src/sakurapool/runtime/compiler.py` 与 BASE 无变更；P2 durable v4 列、COMMIT 和 P3 runtime v1 不改。
- 最终已测试树指纹（`src/tests/tools` 共 54 个 Python 文件，按相对路径、长度、文件 SHA-256 拼接，另含 `pyproject.toml`）：`83d2b12dd78ec85f64693e2997a0e15624ecf0bdc050ccb65313ef04c89ba9df`，完整源码测试前、测试后及 wheel/installed 验证后相同。
- P4 执行单：仓库外 `D:/SakuraTool/SakuraPool-P4-work/SakuraPool_P4_Execution_Plan.md`，不是 Git 提交；README 的未来路径不是 P4 实现完成的证据。详细命令与日志索引见 `commands.json`、`evidence-manifest.json`，两份事实 JSON 见 `canary-summary.json`、`range-benchmark.json`。Git 中不放 token、签名 URL、真实 image/TAR、缓存、wheel 或 venv。

## 2. 实现与契约

- `pyproject.toml` 保持三个 core pins `pyarrow==18.1.0`, `numpy==2.2.6`, `pyroaring==1.1.0`；新增可选 `remote=[requests==2.32.5,modelscope-hub==0.4.0]`。ModelScope Hub 0.4.0 是安装的参考依赖，SDK 未代替受控 transport 发目标请求。Python 3.10/3.12/3.13 通过正常 wheel `[remote]` 依赖解析与 `uv pip check`，无 `--no-deps` 或依赖移植。
- 固定 HTTPS ModelScope origin/repo/type；offline 仅字面 `127.0.0.1`，独立测试 ledger；下载目的域白名单并非凭证白名单。认证只向规定 origin，signed URL、异常、manifest 与报告不记录凭证。ModelScope provider 的版本/文件轻量探测只读且不把无法证实的列表、可变 tag 或 ETag 宣称固定对象。
- 持久双槽预算账：整个 `D:/SakuraTool/SakuraPool-P4-work` 物理 allocation 上限 **4 GiB=4,294,967,296 B**，内存并发待消费 payload 上限 **256 MiB**；目标请求累计上限 2000、实体 body 8 GiB、metadata 64 MiB，重试和 redirect 均计。这些是**授权/账本限制**，并非所有实际路径的全链资源硬界均已证实；尤其不能用 4 GiB 授权自动放行下载。生产 scan/package compiler 和 fetch 在未证明完整物理盘界时拒绝；公开 `fetch_bound_sample`、`fetch_bounded_samples`、package facade、CLI 的生产 fetch 在 condition probe/HTTP/输出/保留预算**之前**明确 `BLOCKED`。不能把合成 fixture 下运行当成生产 fetch 已可用。
- 合成 loopback TAR 走单次顺序 stream、原始字节 SHA-256、P2 4 表及 COMMIT、原 P3 编译与验证、固定 snapshot/binding、image/可选 JSON 精确 Range、SHA 验证及先完整暂存后原子发布；零长度但**存在** metadata 能生成空 JSON 文件，且为 JSON 增加 **0 HTTP**。无 JSON 时 HAS_METADATA flag 与 audit `json=null` 精确一致，双向篡改拒绝。Range 状态/Content-Range/length/encoding/validator 异常离线回归覆盖；没有可证过期原因的 signed CDN 403 fail-closed，**明确过期 URL 一次同固定对象重解析尚未实现**，不能标§8完整。
- 未正式发布的 P4 package format 1 此次补全 schema：`producer_code_version=0.1.0` 是 distribution version，**不是实现 commit SHA**（该 SHA 在本报告绑定，不能 package 自引用）；`durable`/`runtime` 入口、真实 P2 INPUT 衍生的 canonical `audit/scan-config.json` 及 SHA、`scan_evidence=audit/members.json`、`binding_mode=strong_etag_if_match`、coverage `{mode:canary, object_count, full_repository:false}`、`mtime_provenance=unavailable; P2 mtime_ns=0`、实际 image/json member path/offset/size/hash 和唯一文件 inventory。scan config 是已有索引的 adapter/input 配置证据，**不是重新核实目标 provider 的证明**；mode 宣告不是条件请求成功的证明。旧离线 format1 package 与本轮更严 schema 不兼容，必须重建；没有声称已发布 format1 或改变 P2/P3 契约。
- Reader 验 manifest/全部文件 SHA 与相对路径、拒 symlink/越界、P2 `INPUT.json`/全部 `.COMMIT`/4 fragments 的原 strict inventory、与 runtime source fingerprint、对象全集和逐条 P2 样本 batch 行/record、实际对象索引/dataset、member extent、computed **image** SHA 及 HAS_METADATA 匹配。删光 durable 后修改 inventory、增删跨对象、篡改成员 digest 并重算 package inventory、跨对象 runtime location 重 hash 均拒绝，且错误绑定不会先取 image Range。JSON digest 是 P4 sidecar member audit 与回读字节验证，**不冒称 P2 样本行计算了 JSON SHA**。Package open 流式 batch 检查最多 100k 个样本而非 O(1)；静态可信本地无其它 writer 的 canary 文件，`INPUT.json` 与所有 `.COMMIT` 调 P2 loader 前普通文件预检查各 ≤1 MiB，manifest/audit bounded JSON。原 P2 loader 无内建上限；不宣称敌意并发替换下的 stat/read 原子性或整个生命周期硬内存界。

## 3. 唯一真实发现及停机口径

- 只尝试了授权的目标 **revision:null** metadata bootstrap：v1 `2026-09-28T09:43:29Z`，exit 2、账内一次 attempt、body/metadata 0，错误类别**未知**；失败后只结算独立证明未用的 disk-only lease，不凭 body0 猜 HTTP 状态。v2 `10:26:15Z`，本地 Windows `os.execve` 崩溃 `0xC0000005`/十进制 `3221225477`，**没有新增 target attempt**，不能说 CLI 已执行完成。随后以 `runpy` 严格校验 wrapper 离线修复，v3 `10:47:24Z` 使用经授权 child，exit 2，实为 revisions `metadata_headers` **HTTP 404**，账再加一次 attempt，body/metadata 仍 0；404 不能推导仓库不存在、token 错、endpoint 应换、也不能回填 v1 原因。v1/v2/v3 原始日志/evidence 与租约释放证据均在固定预算根，字节/SHA 见 manifest；不把凭证值写入文本或报告。
- 最近**历史生产账证据**为 v3 `discover-bootstrap-v3.evidence.json` 完成 `2026-09-28T10:47:25.274942+00:00`：generation 8，累计 target attempts **2**，已读 raw entity body **0 B**，metadata **0 B**，pending lease **0**，records/saved samples **0**。之后停一切真实请求；没有在最终认证后再读生产账取新证，不声称那时戳以后有实时账快照。早期 184 例离线定向中生产 guard 负例曾错误构造/读取真实固定根 ledger（虽无由该例发送目标 HTTP），之后改为真实 BudgetLedger 类型、`object.__new__`/tmp_path 隔离及所有保留/probe/status sentinel；最终 381 例已在纠正后的树上跑。不能说测试从未触生产账。
- **没有**目标 revisions candidates、frozen revision、目录清单、合格 TAR/source/tag 凭据，未形成真实 canary plan/package；目标 TAR 顺序扫描 0、真实 P2/P3 生成 0、目标 Range 0、真实 sample 0、真实完整 repository 覆盖 false。除上述 ModelScope revisions metadata HTTP 外，**未访问目标数据对象或其它 ModelScope 生产数据服务**；未向目标 repo 上传/删除/更改可见性或为该 dataset 建远端 branch/tag（Git 代码仓库 `origin/dev` 已正常 push），未做全库扫描或生产性能测试。继续目标 HTTP/真实凭证读取均停止。

## 4. 最终树验证和失败保留

- 最终实现 SHA `56f40c43d0a85943b633c38eab6a4175d53b434f`，**唯一** Python 3.12 完整源码命令（cwd repo；`PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1`）：`D:/SakuraTool/SakuraPool-P4-work/.venv312/Scripts/python.exe -m pytest tests -q --tb=short -ra -p no:cacheprovider`；2026-09-28 12:52:59–13:08:19 UTC，exit **0**，**381 passed, 2 skipped, 920.49s**，无 deselect，原 P3 2-million-record fixture 保留。两个 skip：`tests/test_p4_package.py:165` 本机无 symlink 权限 OSError；`tests/test_runtime_remediation.py:787` OS/user 不可建立文件 symlink。原始 stdout `p4-cert-56f40c4/source-full-312.stdout.log` **785 B SHA256 `cba3e0c74f9714e7ca0575239c8498c029e054f2f71892366cb01f4b1434d7b6`**，stderr **0 B SHA256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`**。更早 `7f8` 原完整 suite **362 pass/2 skip 756.69s**、A-D 中途 targeted 184 pass/1 skip 均不能代替最终结果，且部分早期命令仅工具输出无原始日志；A-D reviewer 整合中间 targeted 188 pass/1 skip 有保留日志，最终 cap 小集 31 pass/1 skip。
- `D:/.../p4-cert-56f40c4/archive`（原样 `git archive` 提取）运行 3.12 pinned `ruff check src tests tools/p4_token_bootstrap.py` exit 0、All checks passed；stdout **104 B SHA `8644960c8dc0db24e77e56146b91bab56369303010574d9f4ed85d985e6d1ad5`**，stderr **0 B empty SHA**。`git diff --check BASE..IMPLEMENTATION` exit 0；stdout **89 B SHA `feecb7e65a9eb3a528d330834fe1275cd47ed3305f87375efdca9c2efd830155`**（仅时间/exit wrapper），stderr 0 B。命令实际日志在 manifest，不能把 wrapper 文本冒成工具原 stdout。
- 旧 `7f8` wheel 已建 **94,930 B SHA `71006e4cfaa1376930aa4555ab92cc08faa2d2547b2ece8a540563ef0ffa4bc5`**，首次新隔离 3.12 常规 remote 依赖安装因包请求三次重试超时 exit 2，**不是产品测试失败或安装通过证据**；旧 artifacts 保留且不用于新树认证。新树 `git archive` 生成 tar 后第一次 GNU tar 将 Windows `D:` 判远端，提取 exit **128**，错误原日志41 B SHA `b4c1ae331147eb7c0db3360210e1a898868274225ea3bc987edd3e17b6f0ef0b`；仅改命令 `tar --force-local` exit 0，保留失败日志，无产品更改。

## 5. 最终 wheel、依赖、来源与离线端到端

- `git archive 56f40c4…` tar **1,638,400 B SHA `82865947fe43a70404a5498dbd584b34aaff8772f9a009be44cdb881f489c483`**；`uv build --wheel --python 3.12 --out-dir D:/SakuraTool/SakuraPool-P4-work/p4-cert-56f40c4/wheels <clean archive>` exit 0，wheel `sakurapool-0.1.0-py3-none-any.whl` **98,821 B SHA `6ac2b9742e5f2f30c8d77f714ae340894ee1560aff873692c93cacbd0b15abe2`**。`uv build` stdout80 B SHA `fb28fbad4cbad36fb826fa88f1d070977b49376cb90f2696a3bfe8b9b93b0ded`、stderr7146 B SHA `a69fd0d65443fd5dc7760de0d118c35dfcd40ec16d29c78b83f5fa1b22c54bd6`。
- 隔离 Python 3.10.21/3.12.13/3.13.13 各 `uv venv --python 3.<minor> ...` exit 0；各正常 `uv pip install --python <venv>/Scripts/python.exe <wheel>[remote] pytest==8.3.4 ruff==0.9.2` exit 0；`uv pip check --python ...` 各 exit 0，分别核 24/21/21 包兼容；请求只为依赖 PyPI，不属于 ModelScope 额度，空间仍计唯一固定 root。版本、stdout/stderr **实际字节数与 SHA** 逐条在 evidence manifest，stderr 列表包含完整安装依赖及固定 wheel 位置。没有改全局代理、`--no-deps` 或复制包伪认证。
- 在仓库外 cwd `D:/SakuraTool/SakuraPool-P4-work/p4-cert-56f40c4`、unset `PYTHONPATH` 与 `MODELSCOPE_API_TOKEN`，分别用三环境实际 site-packages import；`installed-origin310.json` **7278 B SHA `5fb0c8e6875abf1619b3d89da19ef93562ae33e7bb7fa87227e5c065a6b2e432`**，312 **7278 B SHA `ef5a54caf959b3bfcf8a520b879a4d4ae4e953c4d56143fd736101f3ba71a3cc`**，313 **7278 B SHA `bfd40d43fab2e49afd519e97a8728a46d9ce6e211ccd4202a0f10aca982f7f58`**。逐一比 13 个 storage/CLI/runtime 核心模块：**archive/wheel/installed site-packages 原始 bytes 完全相等**；Windows `core.autocrlf=true` 的 Git blob 用 LF、git-archive 文件为 CRLF，**原字节并不相等**，仅将 archive/wheel CRLF→LF 后与 Git blob 一致。可核 Git blob 和 wheel 各自 SHA 在 origin JSON；不能写成 Git→wheel 原字节一致。
- 仓库外 cwd、unset checkout `PYTHONPATH` 和 `MODELSCOPE_API_TOKEN`，使用纯 installed 3.10/3.12/3.13 及独立仓库外 `installed-cli-http-cert.py` 合成 harness，**最终可复现三版本集合**为 310、**312-r2**、313（脚本现存 **9594 B SHA `93931f7735c8bd5eebc5d00b49561a92862ad2fc2aa90da39abb34698faa02e6`**）；各执行一次 1 个极小 TAR/2 records，loopback 全流→P2→P3→package→子进程 installed-only CLI inspect/fetch；各 exit 0、**各 4 个 synthetic HTTP calls，最终三次合计 12**，每次分别为 TAR 全流、revisions metadata、repo/tree metadata、**一个**已带 `If-Match` 的 Range；每次目标 HTTP 0，image `b"abc"`/JSON `b"{}"` 正确、fixture 请求无 Authorization。四调用**不含**实时 206 正向能力探针加 412 负向能力探针：harness 的 `ledger.record_condition_proof` 是**合成注入**，此 installed 链验证已有 proof 的校验/复用及条件 Range 的字节正确性，**不能证明现场服务端 If-Match 变版拒绝能力**；其他离线真请求 fixture 回归 `tests/test_p4_transport.py::test_validated_if_match_pair_and_ignored_condition_refused` 与 `test_condition_proof_persisted_only_for_exact_binding` 在最终 381 例源码 suite 中覆盖现场 206/412 成对和不遵条件拒绝；但**生产 ModelScope** 能力依旧未证。
- 最终三版 stdout：310 **2143 B SHA `fd70662c5f19100ff0e2d2149628b100368804e9f649d579bb0a213b91cbd841`**；312-r2 `installed-cli-http312-r2.stdout.log` **2146 B SHA `1f446210d5fa108dc14b1364b2706db1f5c753c26dcc5570c44ee8d3b896a941`**（2026-09-28 13:34:17–13:34:42 UTC，exit 0）；313 **2143 B SHA `90bd26021e959171acf01a48f260cee91ec829c939cfc0b24b96878799699a85`**；stderr 各 0 B SHA `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。312-r2 前后同一脚本 SHA 均 `93931f…`；`installed-origin312.json` 的 site-packages 来源 SHA 均 `ef5a54…`，未重建产品 wheel。**保留历史初版 312**：执行时同一脚本路径在覆盖前的独立读取仅记录了 **9521 B SHA `6f948fdb801f7009323f631826d14d71da657398eac220dc1a9ca7392ac87b52`**，但**初版脚本原字节快照没有留存**，不能拿现在 9594 B 文件当初版；当次 312 stdout **2143 B SHA `d97e11665afd9d669bf1d936253838f55465cbec823dcf52e96c9b4219ecf902`**/stderr 0 B、exit 0 独立保留，r2 是补充可复现新证，**不追溯替换**那次结果。历史 312 同样 4 loopback calls；包含历史的这四次 installed harness 合计 **16** synthetic calls，**不是所有离线测试的总调用数**。该证据仅合成安装来源，不是目标 HTTP/生产性能。
- 另以仓库外 cwd 的 installed 3.12 pytest 执行 2 个 transport/CLI 案例加 `test_p4_cli.py`，**22 passed 99.23s、exit 0**，stdout **224 B SHA `c15e8041adce311492b9ffccd2421050144d6ef32b4e9547261a059ea9951343`**、stderr 0 B；其中测试子进程显式设置 checkout `PYTHONPATH=src`，故**混合来源**，不能代替上一条 pure-installed 子进程认证。没有在 3.10/3.13 假称跑过完整测试集。

## 6. 预算、性能与局限

- 固定工作 root 的物理 allocated scan（**文件系统计量，不是生产 ledger 读取**）：最终 full suite 前 **192,643,072 B**，之后 **192,651,264 B**；新 archive/venv312 安装前 **203,198,464 B**，三隔离安装和 installed/mixed 认证后 **1,183,277,056 B**，最终 312-r2 前 **1,183,277,056 B**、后最近 **1,183,285,248 B**，比 4 GiB 上限余 **3,111,682,048 B**。包含旧 7f8 archive/wheel/失败 venv、所有日志和新 build cache/venv/tmp；不换根、不清未知文件、不重置目标 budget。静态合成测试临时根与真正生产账预算不混报。此口径不是进程 RSS/峰值或完整运行时扫描峰值；没有生产性能测量。
- 目标发现止于 known revisions 404，未获可信 data revision / 版/目录/selected TAR 或实际 source/tag；不能合格地开展生产顺序扫描、Range/canary/P3/fullverify、无本地 TAR 真实用户 fresh-process 取样、并发 1/4/8 生产 benchmark。`range-benchmark.json` 以 `null` 表示未测，不用 0 冒实际测量；`canary-summary.json` 明确未覆盖目标全库。offline fixture 和 3 版本 wheel 安装通过**不改变**状态 `P4_PARTIAL`。
- 固定模式/manifest 中的 provenance 不证明服务端永久不变；明确已过期 URL 重解析尚未实现；production fetch 与 compiler 在未证明盘界前 BLOCKED；包读校验以可信、静态、单 writer 根为边界，不提供恶意并发文件替换原子保证。P2 image computed SHA 可核；JSON 仅 P4 sidecar 自校，不能虚构 P2 computed JSON digest。没有产品性能基线/优化收益；离线 fixture 的秒数、HTTP call 数仅证明小数据功能正确，绝不冒 ModelScope 性能。

**送审结论：`P4_PARTIAL` / `WAITING_REVIEW` / `MERGE_NOT_AUTHORIZED` / `P5_NOT_AUTHORIZED`。** 不自动发起下一批目标请求、不访问真实 token、不合并 main、不启动 P5。待外部 review 对固定实现 SHA 及报告-only 提交单独确认。完整复制报告的提交 SHA/远端状态在 report-only push 后于交付回复补充，避免本文件自引用。
