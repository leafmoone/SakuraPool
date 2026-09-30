# SakuraPool P4-R2 连续送审报告

**结论：R2 接线实现、离线完整回归和必要 installed-wheel/fresh-process 验证完成；真实生产门禁未通过，P4_COMPLETE = NO。** 两个管理员 builder 接口存在且小型离线链路通过，但当前目标/固定工作根下都不标 READY。真实请求批次已经停止；没有继续探针、完整远端 TAR、扫描或 benchmark。交付后停止于 WAITING_REVIEW，不合 main，不启动 P5。

## 1. Git 身份、范围与状态

- `R2_BASE_SHA = 249dc9bebdc3beed40dd81c6067a7a2f8a814ae3`，BASE tree `00133d40710736105972fb2fdd8121737f8a234f`。
- `R2_IMPLEMENTATION_START_SHA = 249dc9bebdc3beed40dd81c6067a7a2f8a814ae3`：实际开始修改的固定父基线；不是新增实现提交。
- 首个且唯一 R2 产品/测试实现提交，以及 `R2_IMPLEMENTATION_END_SHA = b2af1f1b6049129a27dcb9e7a8498c50eda56791`，tree `3e0c4ec02257c364719095942a81a3db0360311e`。
- 实现提交主题 `feat(storage): wire gated Rust ModelScope transport`，18 个显式授权路径，已经正常 `git push origin dev`，`git ls-remote` 核验远端 dev 等于该完整 SHA。
- `R2_SUBMISSION_SHA` 是随后仅 `reports/R2/` 文档/验证证据提交：为避免报告嵌入自身 SHA，以 `git log -1 --format=%H -- reports/R2/report.md` 解析；最终回复给出实际完整值以及全部中间提交、最终远端 dev 核验。
- 本轮 dev 产品提交仅上述一项；后续 SUBMISSION 只提交报告和必要证据，不改变已测产品/测试 tree。
- local main、origin/main 和远端 main 保持 `edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`。唯一 Git 写入方为 swe，审查方只读。
- 第一次 commit 因 user identity 未配置 exit128，没有产生提交；读取历史 Author/Committer 后仅通过本条命令的 `git -c` 沿用历史身份，没有改全局配置、amend、reset、强推或历史重写。
- 保留既有 dirty `reports/P3/raw/ruff.log` 及未知/untracked 工作文件、旧 diff/tar/报告和 `todo.md`，没有因 push 授权把它们纳入提交。MEMORY.md 仍 ignored，不提交。

## 2. 实现与未改变的契约

Python 继续掌握 ModelScope 控制面：repo owner/name、SDK legacy 数字 ID 的实时精确身份核对、受限 tree 页、revision 候选、真实 canonical path/size。数字 ID 不替代公共 repo identity；master 和 hex 回显不是 immutable proof。结构化 `ProviderObject` 包含 repo_id、repo_type、origin、revision、object_path、object_size、validator、cdn_host；Rust 不重新实现 ModelScope discovery API。

Rust 新 `production.rs` 在既有 worker 四个 operation 和 NDJSON v1 envelope 内使用 additive production payload，不设计外部 continuation/逐 hop 协调框架。origin/CDN 是一个逻辑操作的内部两跳状态；启动 worker 前保守预留两次 attempt/body/disk/inflight。显式 production profile 为 `modelscope_https_v1`，只接受配置的 ModelScope HTTPS 原点；loopback fixture 使用独立 test profile，不能产生生产域 proof。

- reqwest blocking + rustls，禁环境代理、自动 redirect/retry。原点接收 Bearer/可选同源 cookie；原点 302 的 Location 经过受限门禁后，以新客户端发起无 Authorization/Cookie/Referer 的 CDN 请求。保留 Range；拒绝下一次 CDN redirect、userinfo、fragment、encoded-host/path escape、scope/expiry 不确定和凭证回显。Cookie 防回显按独立值检查，不只检查整条 Cookie 字符串。
- Range 仅接受严格 206，精确 Content-Range/Content-Length/对象总大小、identity encoding、strong validator；拒绝 200、错误/重复长度、错误 ETag、short/long body。输出真实受限字节文件而非仅 digest；Python 在 SHA/范围核对后才向取样流程提供字节，无 Python production byte fallback。
- 新条件 proof 域要求同对象/CDN、正确 If-Match 严格 206、错误 If-Match 空 412；旧 plain-206/ETag 观察 proof 不授权生产。repo revision、provider ETag、whole-content SHA256 与 1-byte digest 四种事实明确区分。
- Known 消费在失败时仍 charge；只有可信完整 accounting 才 settle。未读的非空/不确定 rejection body、malformed accounting、timeout/death、early parser stop 保留 pending，不一律 refund。download EOF 已确认而随后本地 scanner 拒绝时，已知 body 仍完整结算。
- 正式真实 canary 之后新增仅 phase、数字 status、已读字节、accounting completeness 的脱敏 diagnostic；没有 raw URL/body/exception/token。新 diagnostic 的离线 fixture 通过，但不能事后补出那次真实请求的 HTTP status。
- `download-then-scan` 完成 whole-TAR 本地 spool 后使用旧 file scanner；`remote-stream-scan` 由 Response Read 经 counting tee 直接进入旧 reader scanner，同时写同一 whole-TAR spool。Python 对文件 extent/digest/JSON payload 审计，复用 `rust_index.build_stage_from_scan`、members.sqlite/stage.complete 和旧 P2 writer。最后 marker 仅在完整 stream 与全部审计成功后写入。
- 管理员 CLI `index scan-remote --mode download-then-scan|remote-stream-scan` 先检查新输出路径、exact proof、组合工作集和 capacity，再请求 metadata/body。当前 `--output-package` 实际为新 P2 durable 目录；P3 compile/verify 和发布 package 分开调度，不冒称一条 CLI 自动发布了可用用户索引。
- 有效 local package 验证 frozen P2/P3/audit/object chain 和 proof 后，image/JSON 由 Rust Range 获取；缺包/坏包不隐式扫描。生产 package verification 使用小型 canary admission：≤4096 entries、总文件≤1MiB，无 reparse；预留 `64MiB + 128*total_file_bytes`，先检查 Parquet footer/rows/uncompressed expansion，再 decode，并跨 fetch 持有 lease。CLI 只验证/保留一份 package，先于 config/token 读取。

**没有重设计 P2 durable v4、P3 runtime v1、Rust TAR scanner、ObjectId/RecordKey/snapshot identity 或 package schema。** `rust/src/lib.rs` 只新增 module export；旧 loopback protocol/fixtures保留。Range 合并尊重 Rust 8MiB cap，clone 用同 ledger/对象域。Python 仍 `>=3.10`，原 pins 不变，is_reparse 使用 lstat 避免较老 Python 缺 is_junction。

## 3. 环境与实际命令结果

唯一 keeper：`D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`，普通 CPython 3.13.15。Rust rustc/cargo 1.98.1 windows-gnu，PATH 显式 cargo/MSYS2 gcc，CARGO_TARGET_DIR 为 `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target`，worker 显式为其 release/sakurapool-worker.exe。所有 Python 认证 PYTHONPATH unset；没有第二永久 venv、预算根迁移或依赖升级。

简记 K=上述 keeper Python，W=`D:/SakuraTool/SakuraPool-Fix3-20260930a/r2-wheel/sakurapool-0.1.0-py3-none-any.whl`。

| 命令 | exit | 实际结果 |
|---|---:|---|
| `cargo check --locked --lib --manifest-path rust/Cargo.toml -v` | 0 | crate/lib/依赖路径核对通过 |
| `cargo test --no-run --locked --manifest-path rust/Cargo.toml -vv` | 0 | 7 个测试 executable 生成 |
| `cargo test --locked --manifest-path rust/Cargo.toml` | 0 | **58 passed，0 failed，0 ignored**；doc tests 0 |
| `cargo fmt --check --manifest-path rust/Cargo.toml` | 0 | PASS |
| `cargo clippy --all-targets --locked --manifest-path rust/Cargo.toml -- -D warnings` | 0 | PASS |
| `cargo build --release --locked --manifest-path rust/Cargo.toml` | 0 | 当前 release worker PASS |
| `K -m ruff check .` | 0 | PASS |
| `git diff --check` / staged 授权路径检查 | 0 | PASS |
| remote-stream exact fresh 定向 | 0 | **1 passed，44.15s**，真正新 Python -I/worker 精确 image+JSON |
| `K reports/R2/run_regression.py`，内部 `K -m pytest -q -ra` | 1 | 首次正式 **556 passed、2 skipped、1 failed，1895.19s**，失败保留 |
| 修复后原失败用例定向 | 0 | **1 passed，0.25s** |
| `K reports/R2/run_regression.py --final`，内部相同完整命令 | 0 | **557 passed、2 skipped，1815.63s**，总终态559，无 deselection/排序插件 |
| `K -m build --wheel --no-isolation --outdir .../r2-wheel` | 0 | 当前 keeper 构建 wheel |
| `uv pip install --python K --reinstall-package sakurapool W` | 0 | 正常依赖解析、同keeper noneditable安装，不用 no-deps |
| 仓库外 `K -I reports/R2/wheel_verify.py --repository ... --wheel W` | 0 | **7 passed，123.01s**；site-packages/noneditable，30源码与wheel/current及installed核对，两mode及fresh链路 |
| `uv pip install --python K --reinstall-package sakurapool -e '.[dev,remote]'` | 0 | editable恢复，原pins未变 |
| `K -m pip check` / `K -I -c <actual import source>` | 0 / 0 | 无坏依赖；module回本repo src |
| 实现 `git push origin dev` + `git ls-remote` | 0 / 0 | b2af1f1…远端SHA精确匹配；main不变 |

最终 skip 仅两项：test_p4_package.py:179 本机没有 symlink privilege；test_runtime_remediation.py:787 该 OS/user 无法创建 file symlink。新增36个 R2 acceptance节点全在完整suite运行；wheel的7项是必要 selected acceptance，不叫完整 wheel suite。

专项链路覆盖 origin凭证/CDN清除、Location/expiry、条件正反、strict范围/size/etag/encoding、200/short/long、worker death和pending、新ledger/process精确scope、partial invalid fullstream无stage marker、路径逃逸和工作集先于网络。两mode均完成小合成 TAR→审计stage→P2 samples1/errors0→P3标签query count1→完整package→Rust精确 image/JSON，并在 -I 新Python进程重开同ledger/proof完成fetch。完整同源码 regression 与 wheel 验证后才提交。

首次完整suite失败为普通门禁回归：write_staged_v4 先访问非BudgetLedger stand-in的offline_mode，触发AttributeError。修复为先类型检查并保留BudgetExceeded/BLOCKED错误词；没有改弱既有测试；再次冻结产品 tree，完成上述557/2后才实施提交。Rust一度E0463，非破坏 check-lib/no-run 后重建测试依赖成功，58新实测通过；具体最初原因未知，不凭该错归因并发，不clean/delete target。

期间外部preflight会话与固定P4-root发生temp生命周期竞争，导致部分专项FileNotFound失败。没有kill未知进程、忽略消失路径、重置ledger或继承外部统计。一次后台正式调用仅启动回执无可恢复终态，标UNKNOWN，不声称中断/通过；用户明确要求恢复后使用单次前台捕获runner，保存stdout+exit，正常完成。更早32test/59test中间PASS不替代最终tree认证；cookie error-code fixture的一次失败已保留说明，后plain/encoded/ETag独立值case在最终suite和wheel均通过。

## 4. 一次真实最小批次：停止且 BLOCKED

请求前宣布：控制面最多3个逻辑读取/9 attempts/9,437,193B entity bound；Rust最多3个双跳操作/6 attempts/6B entity bound，每个正请求只Range0-0。总attempt上限15，先metadata身份/真实tree，再观察、正确If-Match严格206、错误If-Match412；任何门禁失败停止，禁止fullstream/scan/benchmark。这是实体读取/账本预留界，不是全部wire bytes或历史总消费的精确上限。

实际命令 `K reports/R2/real_canary.py` **exit3 BLOCKED**，只执行一次：

- 实时repo-info精确身份核对给numeric hub ID 218032。
- master discovery及candidate tree各一页20条，均不完整；实际选中 `pre/gamecg-v1-pre-p02-003.tar`，声明 `1,401,159,680 B`。
- revision candidate `77948d890f654e6151a5ff7597b63a8337d8910e` 是metadata候选/回显，不据此证明immutable。
- Rust原点302、受限Location gate后尝试CDN；结果 `production_error=cdn_status`，attempts2/body0/completefalse。**最终CDN数字HTTP status未保存，为UNKNOWN；不推造200/403/404，不归因token/权限/签名过期。**
- 没有完成观察206/validator，更没有继续正反If-Match；version、validator、conditional semantics、Range以及content SHA256仍unknown；生产验收BLOCKED。旧历史206不能迁移作证明，无已审替代binding，无Pythonfallback。
- 此次ledger attempts16→21，known body25278→46987，metadata25277→46986，即3个metadata+2个Rust attempt、已知metadata+21,709B。Rust reported entity0B并不代表未知消费为0。
- 新增2 pending leases，unobserved body bound2B；保留不reset、不refund。2B仅本批保守额度，不是实际消费。此次body totals含pending为46989B。
- 历史账本记录与未入账累计分开；历史unledgered requests/bytes仍unknown，不重写为0。该批后没有追加任何真实请求。

提交前再只读核对固定root：known attempts21/body46987/metadata46986、pending2/body-bound2、inflight0保持；physical root `2,000,220,160 B`。root占用相较canary后观测增加包含测试/其它既有产物，不能全归本批真实数据。

## 5. 两种 builder 的内存、磁盘与 readiness

固定cap仍 `4,294,967,296 B`，stage reserve和durable build allowance各 `1,207,959,552 B`，inflight cap `268,435,456 B`。不增加额度、不清理既有数据/旧target、不绕过固定根。

Range保守inflight `4*N+65,536`；FullStream/report audit为 `64*object_size+4,194,304`，避免旧scanner报告/JSON展开低估，不冒称常数整条pipeline内存。真实对象所需inflight `89,678,413,824 B`，超cap。两mode都暂存**完整本地TAR**；remote tee不是diskless。

按提交前root观测，组合root+stage+durable+rawTAR+16MiB report+8192B admission = `5,834,084,352 B`，超4GiB `1,539,117,056 B`。即使不计对象与report，root+两个1152MiB leases也达 `4,416,139,264 B`，超 `121,171,968 B`。这是保守同时持有实现的门禁，不等于实测峰RSS/生产吞吐；两个现行CLI都在metadata/body前拒绝。

- `INDEX_BUILDER_DOWNLOAD_THEN_SCAN = BLOCKED`：接口/小离线pipeline通过，但目标binding未知，真实对象spool/report memory和当前组合disk不可行。
- `INDEX_BUILDER_REMOTE_STREAM = BLOCKED`：Response Read→counting tee→原scanner接口及离线fresh链路通过；whole-TAR spool仍需上述capacity，binding未验证。没有真实全流验收。

未引入动态stage-cap、第二budget/protocol、新数据根或无凭证自动降级来伪造READY。若后续需支持目标大TAR，必须在审查和明确范围下解决现有报告工作集/容量及binding，不由本轮自动扩阶段。

## 6. 只读审查与已知限制

提交后 reviewer 对 BASE→实现SHA 只读核查未发现阻断性缺陷，但明确一项非阻断失败语义限制：worker 的 production 失败仍可能以 `ok=true` / `result.production_error` envelope 返回；Python `_call` 没有显式把该字段统一转为 RemoteIOError。部分已知空体拒绝可能因下游成功字段缺失而抛 KeyError，未形成完整稳定的分类拒绝语义。目前 artifact/proof 仍 fail-closed，没有凭此生成成功proof或有效stage，也无生产fallback。该限制按实报告，未在冻结验证后偷偷改源码/削弱测试；后续若修须新提交和相应重验。

## 7. 最终逐项判定与安全声明

| 标签 | 判定 |
|---|---|
| `PRODUCTION_RUST_TRANSPORT` | 显式接口实现、离线PASS；真实部署验收BLOCKED |
| `MODELSCOPE_TWO_HOP` | 离线完整PASS；真实origin302/gatedCDN已尝试，最终status UNKNOWN、完整能力BLOCKED |
| `CREDENTIAL_ISOLATION` | 离线origin Bearer/Cookie→CDN无凭证及独立值echo拒绝PASS；不冒称服务器实际收到header的真实抓包 |
| `VERSION_BINDING` | **BLOCKED**：candidate/ETag/content身份分离，真实条件proof未取得，无已审替代 |
| `PRODUCTION_RANGE` | 离线strict bytes PASS；**真实验收BLOCKED** |
| `PRODUCTION_FULLSTREAM_INTERFACE` | reader/file两接口及离线stage/P2/P3/payload PASS；**真实对象受容量/binding阻塞、未执行** |
| `PACKAGE_FETCH` | source/wheel离线query/完整binding/精确imageJSON PASS；真实package不存在/未认证，不隐式扫描 |
| `FRESH_PROCESS` | 两mode独立 -I Python/worker从同ledger/proof重开package精确bytes PASS；非真实生产索引fresh认证 |
| `INDEX_BUILDER_DOWNLOAD_THEN_SCAN` | **BLOCKED**，上述binding/组合disk/inflight原因 |
| `INDEX_BUILDER_REMOTE_STREAM` | **BLOCKED**，上述binding/whole-TAR spool/report工作集原因 |
| `P4_COMPLETE` | **NO** |

所有耗时为合成离线验证，不是生产性能；没有image decoding、大仓库索引、完整远端TAR读取、真实样本矩阵或benchmark。token/signed Location未写argv/log/manifest/errors；无生产Python字节fallback。未删除未知目录、旧账本、历史证据/用户文件；未重置或退款未知额度。仅新增必要报告/小型terminal证据，无附加日志hash或全目录hash清单。

`WAITING_REVIEW` — `MERGE_NOT_AUTHORIZED` — `P5_NOT_AUTHORIZED`。
最终提交与origin refs核验完成后停止；后续修复/真实验证/large build必须遵守新明确指令，不自动推进。
