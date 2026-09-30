# P4-R1 Final Fix3 · 最终认证报告

状态：`WAITING_REVIEW`。`R1_PLAN_COMPLETE YES`；`P4_COMPLETE NO`；`R2/MERGE/P5 NO`。
本报告替代旧 Gate J v2 的当前认证口径；旧提交与失败日志保留，不改写历史。SUBMISSION 为单独承载本报告/原始证据的提交，由 `git log -1 -- reports/R1-final/report.md` 得到，完整 SHA、push 后远端 SHA 与证据 blob 核验在连续交付正文中列明；不能在其自己的内容内嵌入自己的 Git SHA。

## 1. 范围与身份

- 固定起点 commit：`b7c1bd19463cc08e02aaaab37e90c9b696245531`；其 tree：`d13e8bb51ce590aebd8d8a4f2ce5ba24ffd19744`。旧报告把该 tree 当 HEAD 的写法错误，现明确纠正。
- 本轮唯一实现 commit：`51ea23a4367cb8f069e930c5ab1882a0a00c8423`；实际认证 implementation tree：`f0680c67652afc6cb04a236169ecc37bd425aa2d`。
- 本轮从固定起点到 SUBMISSION 只有上述实现提交和单独证据/报告提交；无其他实现中间提交、无 amend。实现已经正常 push；认证时本地 dev 与 origin/dev 均为上述实现 commit。
- local main、origin/main、远端 refs/heads/main 均为 `edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`，未合并。
- 范围：reqwest 成熟阻塞 HTTP 客户端；真实 FullStream Read；stdout 有界队列/行与生命周期；获批最小 TAR 扩展分配前上限及 closing 验证；worker 禁隐式 retry；全新正式 Python/Rust 验证及 installed-wheel 闭环。
- final wheel/full suite/Rust final 均绑定同一实现 tree、同一 release worker SHA；认证后 `git diff IMPLEMENTATION -- src rust tests pyproject.toml` 为空。之前 WIP/precommit 日志仅为过程证据，不拿它们的 baseline HEAD/tree 冒充 final tree。

### 历史 R1 与剩余门提交（旧→新，背景而非本轮新统计）

| 完整 SHA | 内容 |
|---|---|
| `a3ae457c4f6f69cd85fba2b29980b88016550d4d` | Gate A：单 crate / pinned toolchain |
| `55da5dbab66018c66fa6f04773776264d26a5ca6` | Gate B/C：NDJSON / Python durable budget |
| `09c8ce4427391de6640d026b5ac7e1cda795ddb5` | Gate D：顺序 TAR scan |
| `502427d3fc5e0f334e5efe0d6509a02e83e08fb4` | Gate E：P2/P3/Range 闭环 |
| `0a16594e0089a6c6747e4a8a33dcbb91376fd270` | Gate F：timeout / stderr |
| `af6c8910bde127c177b0ecdc80162525e3e60c09` | Gate G：durable crash recovery |
| `398c31a3c1b4fbb57b9e0420a777873823776f48` | Gate H：环境 / offline |
| `da5108930eb6b501324b9ec17049deda57e0c558` | Gate J v2：单 P 环境变量 |
| `ba8c5c541ac5bc9d6b460288c409c275f0e5b1cd` | 剩余门 1：共享 Read scanner |
| `f079c7dcba57489dfe203855a7e3617347dc7101` | 剩余门 2：旧手写 HTTP（本轮替代） |
| `3a1f09bb0b7356454318b7cfc18e06b7aa3bb5d4` | 剩余门 3：HTTP scan |
| `47af09b212f5f93ab10d74e7a05ed7684aafadcb` | 剩余门 4：共享 stage builder |
| `6efbeefaad87fc3690b579e9226bdd417c7c2f9a` | 剩余门 5：worker 有界输入行 |
| `b7c1bd19463cc08e02aaaab37e90c9b696245531` | 剩余门 9：调度约定 / 本轮固定起点 |

## 2. 实现及契约变化

### RUST_HTTP_CLIENT / REMOTE_STREAM_NO_FULL_BUFFER：PASS

依赖精确为 `reqwest = { version = "=0.12.28", default-features = false, features = ["blocking", "rustls-tls"] }`，保留 `tar=0.4.46` 成熟 parser。产品不再手写 TcpStream HTTP；TcpListener/TcpStream 仅合成测试服务器使用。

Client 明确 `.no_proxy()`、`redirect(Policy::none())`、`retry(never())`、HTTP/1、connect/整体 body timeout；localhost 显式解析到 127.0.0.1。TLS feature 显式 rustls；运行 URL 契约仍只允许带显式端口的 loopback http，未因此开放 https/外网/生产。

手动重定向每跳重过 loopback 校验、有界次数、调用方 header 不跨跳；库显式 retry 保留，FullStream 在交给消费者后不 replay 部分流。修复 URL canonicalization 去掉 `:80` 后的重解析问题。默认端口修复为静态复审覆盖，未实际监听 80 专项测试；不冒称该专门场景运行通过。

`HttpBody::Stream` 拥有 reqwest Response，实现 `Read`；`scan_http_tar` 直接传给 `scan_tar_reader`，不收集完整 TAR body 为 Vec。Range 可 materialize 但硬界 **8 MiB**，超界在 connect 前拒绝；Probe 为 HEAD、零 body。旧 HttpResponse.body Vec API 改为显式 HttpBody / bounded_bytes，这属于 Rust API 变化，NDJSON v1 不变。

共享扫描器查固定 tar0.4.46 实际 API 后采用 `entries().raw(true)`，成熟 parser 负责 header/checksum、entry geometry/padding/PAX record syntax；GNU longname / local PAX **先查 entry.size≤64 KiB 再分配/解析**，不是事后大小检查。合法长名/合法 PAX path 与匹配 size 保留；冲突 PAX size、global PAX、longlink/sparse/link等 fail-closed。pending extension 无后续成员拒绝；要求两个完整 zero closing blocks，后续尾部只能零。

此为明确的 Rust 接受域收紧；Python 既有 tarfile 扫描器不随之修改，可能接受 Rust 拒绝的全局 PAX、size override 或缺 closing block。双模式既有合法语料等价仍由回归验证，**不声明两种 parser 对所有 TAR 接受域一致**。

### STDOUT_BACKPRESSURE：PASS

Python stdout queue 最多 **8 行**，单行读取最多 **64 KiB+1** 用于超界检测；队列满阻止继续读下一行，不丢弃正常序列。8 行队列加一个 producer 当前行是有界协议载荷（另含消费者当前行及运行时/OS开销），不是整个进程 RSS 上限。stderr tail 保持64KiB。

kill/wait 解除 blocked stdin writer 后才 close；stop 释放满队列 producer/等待消费者；wait/reap、关闭三个管道、join stdout/stderr 线程；constructor failure 同样回收；重复 close/cancel 安全。EOF done 与末行入队竞态先重查队列，不能丢最后一行。

worker Range/scan HTTP policy 明确 `max_retries=0`；503 两种 op 各只观测一次 GET，防止一个 admitted 请求暗做 retry。库显式 retry 测试保留。未重写全局 durable 账本；既有 partial-body rejection / redirect-hop 的全面逐字节逐跳会计不是本次完成声明，生产 transport 接入仍禁止。

## 3. 新旧环境 / artifact 来源

仅按 root 明确批准使用新工具根 `D:/SakuraTool/SakuraPool-Fix3-20260930a`；创建前检查新路径不覆盖既存路径。新 target `.../rust-target`，editable 正式 venv `.../py312`，fresh noneditable wheel venv `.../wheel312`，wheel 目录 `.../wheels`。新 wheel/data子路径另有不存在 guard，日志可核查。

旧 target `D:/SakuraTool/SakuraPool-P4-work/rust-target`、旧 P4-work/数据/账本全部保留、不移动、不清理。本轮例外仅在 PROJECT_RULES 的固定构建路径条款注释；工具依赖/target/venv 与应用数据工作集分开计量，**数据根、4GiB预算、1152MiB stage reserve、累计账本不变**。

实际环境：Python **3.12.13**；pytest **8.3.4**；ruff **0.9.2**；build **1.2.2.post1**；requests **2.32.5**；modelscope-hub **0.4.0**；pyarrow **18.1.0**；numpy **2.2.6**；pyroaring **1.1.0**；sakurapool **0.1.0**。Rust/cargo **1.98.1** windows-gnu；uv **0.12.5**；MSYS2 gcc **16.1.0**；Git **2.54.0.windows.1**；Windows11 build26100。

| artifact（SHA-256） | bytes | hash |
|---|---:|---|
| 新 release worker `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe`，来自上述 implementation tree、locked release build | 4203343 | `0b8bafc4f1388ad325be68721b421c2865c6bab7af339204af14a7dbea8d8d6a` |
| `rust/Cargo.lock`（工作树与 implementation Git blob 实际相同） | 35107 | `b19a8381aed8e30effea200ffcb4c64acbc2c33a1098ff69aa0ee94961d9da6e` |
| fresh `sakurapool-0.1.0-py3-none-any.whl` | 118494 | `71b5f86f6df952d18bc9590b4728562bb9388bd9c66c57fca6a17ca1e8cd5262` |
| 旧 worker（仅只读核对，非本次认证 binary） | 728160 | `2354779abcb2e043964846bdf413e0f97ee0a943a861804f89497f093dc1d9df` |

初次 cargo check exit101 为 ring 找不到 gcc.exe，未伪报通过。发现既有 `C:/msys64/ucrt64/bin/gcc.exe` 后显式 PATH/CC 修复，无清理旧target。最终所有 worker-dependent 验证显式设 `CARGO_TARGET_DIR` / `SAKURAPOOL_RUST_WORKER`，`PYTHONPATH` unset。

## 4. 新执行命令、退出码与真实统计

所有认证命令完整 argv、cwd、时间、commit/tree、环境和 exit 均在 raw log；简记 `ENVROOT=D:/SakuraTool/SakuraPool-Fix3-20260930a`，`EV=reports/R1-final/evidence`，Python 为 `ENVROOT/py312/Scripts/python.exe`，worker为新release。

| 命令 | exit | 实际结果 |
|---|---:|---|
| `cargo fmt --check`（rust目录） | 0 | final无diff |
| `cargo test --locked` | 0 | **58 passed**：lib6、worker行读取6、http_client10、http_stream7、tar_scan13、tar_stream_bounds4、worker_protocol12；doc0，0失败/ignored |
| `cargo clippy --all-targets --locked -- -D warnings` | 0 | final无warning |
| `cargo build --release --locked` | 0 | 新release SHA见上，不是旧target |
| `cargo tree --locked -e features` | 0 | reqwest blocking/rustls实际feature graph保存 |
| `env HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 ALL_PROXY=http://127.0.0.1:9 NO_PROXY= cargo test --locked --test http_stream --test http_client` | 0 | **17 passed**；故意坏proxy不影响loopback请求 |
| `python -m pytest`（R1 bridge/backpressure/attempts/loop/e2e/env/audit 七文件，-vv -rA） | 0 | precommit **56 passed，237.50s**；正式全套再次覆盖相同用例 |
| `python EV/fix3_run_full.py` → `pytest tests/ -vv -rA --durations=15`，order-only插件 | 0 | **521 passed，2 skipped，3024.01s**；523节点、无deselect/失败 |
| `python -m ruff check .` | 0 | final recheck All checks passed（含evidence脚本） |
| `uv build --wheel --out-dir ENVROOT/wheels` | 0 | fresh wheel |
| `uv venv ENVROOT/wheel312 --python 3.12`；`uv pip install --python ENVROOT/wheel312/Scripts/python.exe 'ENVROOT/wheels/sakurapool-0.1.0-py3-none-any.whl[dev,remote]'` | 0 / 0 | 全新noneditable安装 |
| `ENVROOT/wheel312/Scripts/python.exe EV/fix3_wheel_loop.py --worker ... --data-root D:/SakuraTool/SakuraPool-P4-work/fix3-wheel-51ea23a-a --implementation 51ea23a4367cb8f069e930c5ab1882a0a00c8423 --tree f0680c67652afc6cb04a236169ecc37bd425aa2d`（仓库外cwd） | 0 | fresh闭环 PASS |
| `python EV/fix3_verify_binding.py` | 0 | final代码未漂移；全部523 outcome齐全；wheel的28源码与commit仅允许CRLF→LF一致 |
| wheel解释器只读逐文件检查 | 0 | 安装的28个.py与wheel **原字节精确一致**，不归一化 |
| `git diff --check` / `git diff IMPLEMENTATION --exit-code -- src rust tests pyproject.toml` / `git ls-remote` | 0 | no code drift；实现已push；main不变 |

`PY312_FULL_REGRESSION PASS`、`RUST_FINAL PASS`、`FRESH_WHEEL PASS`。旧509/47或旧wheel统计不充当本轮证据。

实际 skips：
1. `tests/test_p4_package.py::test_package_fails_closed_on_invalid_inventory_or_binding[symlink]`；`tests\\test_p4_package.py:179`：host lacks symlink privilege: OSError。
2. `tests/test_runtime_remediation.py::test_staging_audit_rejects_symlink_entry`；`tests\\test_runtime_remediation.py:787`：this OS/user cannot create file symlinks。

### 显式排序与盘额度限制

获 root 本轮专项授权：fresh wheel先，完整suite只把 `test_p4_two_hop.py` 的83节点放最后。插件不deselect、不改变fixture/result、不调预算；JSON保存排序前后完整nodeid列表，Counter多重集（含重复）相同，均523；binding检查521 pass+2 skip逐节点合并等于该多重集。pytest真实exit0正常传播。

只读固定根物理allocated：正式认证前 **3079282688B**，后 **3102572544B**，增 **23289856B**；offline-twohop目录 **319→329**。fresh数据和全部新目录保留，未清理。结束时固定根仍小于4GiB，但再加1152MiB stage reserve的margin为 **-15564800B**；**不保证当前根还能重新admit stage**。本轮明确排序才完成一次全套，不代表抬预算/通用解决泄漏；后续需新的明确决策，不可自行清理/重置。

## 5. 专项证明、性能边界

- Lazy FullStream：测试服务器只送header、暂不释放body，http_request必须先返回Stream；whole-body materialization无法满足此测试。
- 增量扫描：unsafe第一header送达后立即拒绝，剩余body尚未释放；截断body fail-closed、不重放。
- 16MiB单成员合成TAR：server64KiB固定chunk、scanner固定chunk，整包/成员SHA、offset512均对；fixture自身不建whole-TAR Vec。close-delimited body cap+1拒绝；Range>8MiB connect前拒绝；foreign redirect拒绝。
- tar_stream_bounds四项：GNU/PAX超64KiB只读512B header，payload一旦读取即panic的reader证明预分配保护；合法PAX path/size；合法GNU长名由既有tar_scan覆盖；dangling/truncated metadata及0/1/不足2closing blocks/nonzero tail拒绝。
- stdout新增10项真实子进程/管道测试：慢消费者200行有序、满8行队列cancel/close、等待消费者cancel、blocked stdin writer的close/cancel、EOF末行竞态/正常EOF、16MiB超长无newline输出、constructor timeout回收；核查进程退出、两reader不存活、stdin/stdout/stderr均closed。503 no-hidden-retry另2项，正式全套都PASS。
- Fresh installed-wheel闭环：site-packages实际路径、direct_url archive_info非editable、无PYTHONPATH、仓库外cwd；合成TAR **10240B/2成员**，HTTP whole SHA `38dfdb990ecd410c7894c3533df10b28fc934e48c07c868f811d8706bf029eb5` → product DatasetAdapter →共享build_stage_from_scan →真实P2 durable v4（samples1/errors0）→P3 runtime tags查询（rid1）→Rust gated Range **bytes=512-581 /70B**，SHA `c414cd0e204de974f73753c7e28d7638e7b3691bb8b1a2bab6b25bb7fed7ce77`，与原PNG切片逐字节一致；server共2GET（scan一次+range一次）。Range durable账：attempts1/body70/inflight0/records1。合成scan本身为直接worker request，不冒称它经过durable scan admission。
- FullStream无全量buffer仅声明 **Rust HTTP body→scanner 路径**。Python stage builder仍以小型raw bytes审计，本闭环不证明整个Python生产pipeline零全量内存。member结果Vec受max_members约束；reqwest/tar/runtime内部开销未作为绝对RSS测量。无RSS曲线、无生产吞吐/生产性能结论。全套3024.01s是回归耗时（含既有big32合成fixture），不是生产基准。
- 两轮只读critic：初轮发现blocked write/EOF/extension/closing等后修补；复审限定文件静态无阻断项。审查配置/model/reasoning不可见，不猜测；静态意见不等于测试PASS。

## 6. 原始证据 bytes / SHA-256（Git blob口径）

路径均位于 `reports/R1-final/evidence/`。35份raw log共 **263105B**。capture拒绝覆盖已存log，保留完整combined output与真实exit。`.gitattributes` 对本目录 `* -text` 防止Git换行转换；提交前后逐blob核验，与以下raw字节哈希一致。含失败/早期WIP日志，**不删除、不洗白**：cargo环境101、首次cancel期望竞态exit1、两轮evidence脚本E501 exit1；修补后的final命令单独PASS。暂存全部raw证据后的默认 `git diff --cached --check` exit2仅报告原始stdout尾空格/Windows CRLF；未为消警修改raw字节。非log交付文件用 `git -c core.whitespace=cr-at-eol diff --cached --check -- <report/plan/辅助源文件/collection-json显式清单>` exit0；产品实现提交自身标准diff-check已通过。wheel仓库外capture的自动HEAD字段为not-a-git-repository，保留原文；其显式implementation/tree与独立binding/安装字节核验补足来源，不把自动HEAD错误当产品通过依据。

| raw log | exit | bytes | SHA-256 |
|---|---:|---:|---|
| `fix3-binding.log` | 0 | 1207 | `469db3662aa5eb6299edb341d357d3153e8b6d122519538322d1ef529b225bcc` |
| `fix3-build-final.log` | 0 | 497 | `8303cd970d78bb6d39bf3981c3c1752287704a4404b0d015fb9f99ab1a55ae93` |
| `fix3-build-precommit.log` | 0 | 3789 | `84a041a859a1fd4989ec833fdb4d4cd37f594c2894ea65d3aea5419ae3850470` |
| `fix3-cargo-check-initial.log` | 101 | 13053 | `c2f3e8c10992eee181beb2801530c61077ba0ff9027f62861226ab93e3f6781c` |
| `fix3-cargo-features.log` | 0 | 50236 | `6b37bf1c7ddc426ab0014764d472d9902be0c28e69dd7c175be6a7edf07f6e83` |
| `fix3-clippy-final.log` | 0 | 527 | `5f4b9a736e80bea622c511042aa1537e35d65a57c5ff51cccbeb953621b5bfd7` |
| `fix3-clippy-initial.log` | 0 | 1352 | `a3f30050cf2abdb528553d504f3df4e045bfab8c90c9faf9286932adf5cac1e5` |
| `fix3-clippy-precommit.log` | 0 | 595 | `44d0f41d0ad237b4ba18222f18a02428dd98082845b7b287ccee57756f3ad86f` |
| `fix3-environment-after.log` | 0 | 2289 | `a14b30303bd100add993adaf0deb5cde8ec58ff13f2d43cd034f576b9fe25d7f` |
| `fix3-environment-before.log` | 0 | 2114 | `a8cfefbb36824588722dda8ebb87d2e17feb78c7d01cffd1358beceda02fb83a` |
| `fix3-fmt-final.log` | 0 | 422 | `3a34ad8005fc8ef5fb59c29f375a2b168358778be1f61748e28653ec10724cfa` |
| `fix3-full-py312.log` | 0 | 112257 | `96d6eaddf14d7cd2628d82c60a39e31471a966555fb78b91e4b05fdf5a52fd7d` |
| `fix3-http-proxy-proof.log` | 0 | 2142 | `4ca86d4b6b4fab6170a2f9626f4747282cb8d543cb225b727234aebf4836b513` |
| `fix3-implementation-submit.log` | 0 | 2233 | `e8e425ca97af90ca48ec900a3ce798b273c3993c221021c6dc1817e9fdb1175b` |
| `fix3-installed-bytes.log` | 0 | 1791 | `599ce2add4fe3285eeda4874e4fa3193f69ccf0008f2c01611c0352ab248f050` |
| `fix3-preflight.log` | 0 | 1535 | `36fa0b40001a75353a60ca782c98d168832667d41ee953e4e634fb0c4f7c1e44` |
| `fix3-r1-precommit.log` | 0 | 10810 | `e5b685c00a340458e3a734da34a52183a82ea0b668e3d069345632212a294d70` |
| `fix3-ruff-final-recheck.log` | 0 | 499 | `3280544a001ce3d80b66bd1684ec0de94757b0c0270261de470457321415dc6f` |
| `fix3-ruff-final.log` | 1 | 1019 | `ab195c47c04c28cf76da55badf2e5801698a03799216ed99dbfe617a229f0485` |
| `fix3-ruff-precommit-recheck.log` | 0 | 448 | `86cf9be221068c6e1334cceead363c4078cccb3dd197010577f2f82259df4ab3` |
| `fix3-ruff-precommit.log` | 1 | 1549 | `f1cad6a64eb3978332fcf0de3e20b7c74c4f876d6b7625e6ac6dc9615601dd99` |
| `fix3-rust-final.log` | 0 | 5888 | `7a0575813040f05d43f5a0eb4d4c4025dd6f3095fc682b69f31e4b6ea89a4971` |
| `fix3-rust-precommit.log` | 0 | 5888 | `652530ce685174dbf993f8b4877b84d05e990ab0bc8cff4be47988dac2a003c3` |
| `fix3-rust-tests-expanded.log` | 0 | 5954 | `5b6fcbcb5e5181c199734d0bb3adcfbc04ec63439ef5778da400b67e7c23ea3d` |
| `fix3-rust-tests-initial.log` | 0 | 8127 | `9d278c672b77b302cd27f45cb03d3912c5e40d65a6b75d748614221db3313fe8` |
| `fix3-status-before-evidence.log` | 0 | 1749 | `bc0490521faaf80c7c734f177c2bbcebf37253e77d3ed5cd7c27b12c345b8efa` |
| `fix3-stdout-tests-expanded.log` | 0 | 3220 | `febbf90ef012a6849fea956b9f79bf49e8c1cf4d55632c2b5f806a3f7edf8a39` |
| `fix3-stdout-tests-initial.log` | 1 | 4085 | `10a9d2d03800d3cbc4293433f8215c1fb817daf5c4f83b5d222ceec00cc04ab3` |
| `fix3-stdout-tests-recheck.log` | 0 | 2550 | `e2373ab810f4f9450122f459eccf826ec77b949c8d381458f62113a256592b2d` |
| `fix3-venv-create.log` | 0 | 565 | `fb9345bea0cc0d73cec8147d57c4f717014e3c074b7249d110213d45bf619337` |
| `fix3-venv-install.log` | 0 | 1533 | `2f11a09ed4b3077817589ca1a15341e8d824df8ba4ee1b9cf5f092625094a0b8` |
| `fix3-wheel-build.log` | 0 | 8080 | `30ec951c51a58367e5dd70b7f8683810d89a7466909d6605d6219eedd09d34f2` |
| `fix3-wheel-final.log` | 0 | 2813 | `8bd0cfd417a08f3c28392d9a5699c3187c42e63b39f4e3ce0a911d0257c388f5` |
| `fix3-wheel-install.log` | 0 | 1634 | `f55ca76420c08454f0ccaec20cf4790201ffc2bee5dad7a64b38a29022622cc6` |
| `fix3-wheel-venv.log` | 0 | 655 | `ccf51cee410f30969b292bf38986670185d574a49a701583b8dc8cd0c7bae3e7` |

排序审计/脚本也按原始Git blob bytes固定：

| evidence辅助文件 | bytes | SHA-256 |
|---|---:|---|
| `.gitattributes` | 157 | `a4405f0289a0dbc2e43a93b888dc3aa723b6466c7d8007524c1a55f34e8d808c` |
| `fix3-capture.sh` | 802 | `1d05fbcc1caa2cfb3ef631d733296c03f8a62a6d622a8fc74c3b25d069f92387` |
| `fix3-full-collection.json` | 103446 | `198b5c17b5db3a0df704435657d085941d318f9251b525430196a2be9a9d51a3` |
| `fix3_collection_order.py` | 1555 | `5f8af74c3b537db90a85b5704d64834b09c23cdcb2503b4d421875fb2ed0b4f8` |
| `fix3_environment.py` | 2170 | `baf9394bd3b9c9c9748aa23482f39cb56d02ca58652f1b3000c53edb26cee06c` |
| `fix3_run_full.py` | 902 | `be4beb8ce32715436727556a54bf5b36b27a70c6e1f501e418f1414d45b23d0a` |
| `fix3_verify_binding.py` | 3338 | `a7b1eb86eb75cde91b405763428194a23bf9003f77f9cc5947bb5b4d8992eb9c` |
| `fix3_wheel_loop.py` | 8164 | `4629e61f724802237d087be945f310e6270beb470b9f56734936a5cdbc03d14f` |

## 7. 历史违规、保留项与停止点

明确更正：此前删除的是 **38** 个旧 `offline-twohop-*` 目录（10+8+20），不是18；固定根目录数357→319。第一批10个名字保存在当时 `/tmp/victims.txt`，后28个准确名字/时刻未保留，不编造；Git Bash unlink未过回收站，无可核验备份，不能证明每个已删目录内容。幸存样本不能替代删除内容。本轮不以已删/再生目录作证，不再cleanup，不重写历史；报告保留此违规。

既有dirty `reports/P3/raw/ruff.log` 和全部既有untracked工作文件（P3计划/raw、P2 tar/diff、agents.md、P4 patch/verification、todo.md）未纳入提交、未覆盖/清理。MEMORY.md受.gitignore忽略，本轮纠正其中“可自行清理旧账本”和旧客户端/旧binary误导，保留最短入口与限制，不作为送审代码。

安全边界：仅小型loopback/合成fixture及原完整回归；无真实TAR/生产数据/ModelScope数据服务访问。安装modelscope-hub仅依赖安装，不能视为数据服务访问授权；包依赖下载和正常git push是授权开发网络操作，因此不误称所有网络完全离线。未启动生产扫描/大规模数据任务，未改main，无R2/P5，无reset/rebase/amend/强推/远端删分支/未知文件清理。未使用git add -A或git add .。

六项认证 PASS：`RUST_HTTP_CLIENT`、`REMOTE_STREAM_NO_FULL_BUFFER`、`STDOUT_BACKPRESSURE`、`PY312_FULL_REGRESSION`、`RUST_FINAL`、`FRESH_WHEEL`。完成本轮证据正常push/远端核验后停止于 **WAITING_REVIEW**，不自动执行下一阶段、不把R1完成写作P4生产完成。
