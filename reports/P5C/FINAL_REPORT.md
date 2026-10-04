# P5-C 最终阶段报告

## 状态、范围与身份

P5_C = WAITING_REVIEW。这是阶段送审，不是用户验收、main 合并批准或 P5-D 授权。原 sole 已有限核采纳性能与真实闭环证据；最终文档有限核通过（含口径更正）；阶段仍 WAITING_REVIEW。

BASE_SHA=`fa0c81f286a4755d44319cd94a213e82c275fe26`。
IMPLEMENTATION_END_SHA=`4241e4a538622f9e036992101f67baf1ca81382e`。
IMPLEMENTATION_TREE=`336a33a48f831f8bc27432a0161d9aff65196449`。

报告写入前工具实核 dev/clean，origin/dev 等于4241，origin/main 等于BASE。报告若另提交，报告HEAD不等于产品实测提交；最终报告提交与远端SHA在交付正文给出。未改main、未amend/reset/rebase/force/rename/delete。阶段完整实际提交顺序：

1. `5206b73d139a6c894bad3a2b6372ffc6568d1985` C0 凭证/单次Range materialization。
2. `f642aa6cba19a303f74319ea7a35e4b29471fc84` C1 bounded persistent worker/独立client pools。
3. `2681e27cefcb0442603bb10de063463818ee5b3e` C2 owner accounting/bounded lanes。
4. `6e5ddd4dcbe55ed43bfa3273621855836c40dee9` keepalive shutdown重连测试；先提交后原sole审PASS，属于审查顺序偏差，未amend。
5. `4241e4a538622f9e036992101f67baf1ca81382e` primary/first-secondary及独立lease cleanup修复。

候选review tree不冒commit。P2/P3/publication=4/2/2、TaskDB schema、ledger格式/caps不变；预算v2、large-member/range coalescing、P5-D均未开始，索引机器操作0。

## 实现与安全契约

C0在IO前拒显式credential/config失败，Range只materialize一次且保持payload owner。C1复用有界generation Rust进程与origin/CDN独立pool，累计job credit、bounded request history，不以历史proof跳fresh object proof。C2 coordinator唯一持有真实ledger、TaskDB及权威SQLite；lane以RPC/bounded queues/immutable descriptors执行。

顺序：reserve→OUTPUT_RESERVED ack→IO/fsync/hash→PREPARED durable ack→rename→PUBLISHED ack→owner settle→SETTLED durable ack→DONE。rename不是FS/DB原子事务；无持久交付/结算证明保持UNKNOWN、不自动重试/退款/覆盖。metadata在Python SQL materialization前限定必要投影/type/length/NUL，216KiB字段模型不是SQLite cache/RSS硬限。completion bookkeeping O(workers)，诊断不覆盖DB权威delivery。Range一次性worker32MiB与payload2×length独立准入；persistent resident/payload分离、stream128MiB不变。异常保raw时保payload quota，不猜任意exception graph；真实worker退出证明后才释放对应资源。多cleanup保primary/first-secondary，secondary固定allowlist。

## 验证与失败

最终Python产品验证绑定4241/tree336；Rust门在keepalive提交，之后Rust未改；报告only不重认证。keeper Python3.13.15，真实隔离Python3.10.21，worker `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target/release/sakurapool-worker.exe`。

|门|结果|exit|
|---|---|---|
|最终Python full|1400 passed/7 skipped/2791.01s|0|
|受影响组 tree336|250 passed/1422.02s，Ruff/diff PASS|0|
|实际Python3.10相关组|150 passed/1154.27s|0|
|fresh noneditable wheel|186 passed/1340.08s，仓库外cwd/-I/noPYTHONPATH/sitepackages来源与pipcheck实核|0|
|Rust fmt/test/clippy/release|63 tests，locked GNU工具链各门成功|0|

命令：`python -m pytest tests -q -p no:cacheprovider --tb=short -rs`，定向同flags；`python -m ruff check .`；`git diff --check`。Rust：`cargo fmt --manifest-path rust/Cargo.toml --all -- --check`；`cargo test --manifest-path rust/Cargo.toml --locked --all-targets`；`cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings`；`cargo build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker`。共享target及正式预算操作串行。

七skip：local_partition_builder:337 symlink WinError1314；p4_package:179 symlink权限；p5b_prerequisites:15 Windows literal questionmark；p5b_prerequisites:204 symlink权限；query_batch_compat:65需要native getlimit-absent runtime；runtime_remediation:787 symlink权限；task_safety:86 symlink权限。不是缺worker skip。

必要失败：首full 8failed/1376passed/7skipped/2581.11s exit1（两hostname fixture不满足fullverify、异常payload retention预期、shutdown uncertainty包装及坏checksum retention）；修后250/full如上，不泛称旧测试。3.10初缺pytest/requests及两次90pass/60errors错误worker env；修isolated env/bash export后150成功。wheel初缺setuptools75.6.0 exit2；修临时依赖后成功。BASE构建初缺GNU dlltool/gcc PATH，MSVC误用Git linker；按locked GNU+ucrt64 PATH独立build成功。旧stub/pytest wrapper/teardownwall不是性能证据。small8 raw未同步触发publication_image_sha，不改SHA迎合。统一instrument A两次FETCH_UNCONFIRMED因Meter proxy违反BASE BudgetLedger isinstance门；改为包装真实ledger原bound methods。第二次首失败未定位已启动为纪律偏差，失败独立保留。每操作status仪表曾使wall176.2846s，调试剔除；formal使用lease映射无额外statusIO。

## 代表性性能：负结果

mixed8：2对象×4成员，image实际1024B×8=8192B，metadata0。每sample独立server/ledger/task/output，固定三轮A→B→C→D；warm不纳formal。A为BASE自身串行API/source/tests和独建worker；BCD最终源码workers1/2/4。BASE tree `ed0f9d492ced24ad8ea2b272ca47c5ae7b2dac57`，source `C:/Users/PC/AppData/Local/Temp/sakurapool-p5c-base-4241`，worker `C:/Users/PC/AppData/Local/Temp/sakurapool-p5c-base-target-ucrt-4241/release/sakurapool-worker.exe`。BCD source `D:/SakuraTool/SakuraPool-p5a-clean`/tree336，worker为Fix3 release。

主wall run_task入口→返回+transport.close；setup/pre-run/post-filecheck独列。原verify字段包含publication/server/transport setup，非纯verify。exactlookup/downloadURL替换，故实际Rust两跳proof/Range/ledger/task热点，不是完整provider E2E。job819951ab exit0，12formal均COMPLETED/delivered8/finalinflight0，冻结seq0..7；实际8file各1024B、SHA `785b0751fc2c53dc14a4ce3d800e69ef9ce1009eb327ccf458afe09c242c26c9`匹配receipt。未实跑性能export，只称冻结seq交付一致。

|sample|setup s|pre-run s|wall s|spawn|origin/CDN TCP|
|---|---:|---:|---:|---:|---|
|A1|2.0902351|.0599579|84.4823425|14|14/14|
|B1|2.1006252|.0633973|87.7851784|1|1/1|
|C1|2.1213861|.0628483|119.2810620|2|2/2|
|D1|2.0952084|.0665305|177.8998640|4|32/32|
|A2|2.0926823|.0588938|92.1165890|14|14/14|
|B2|2.1829203|.0659634|92.1799389|1|1/1|
|C2|2.1493119|.0645661|128.3257196|2|2/2|
|D2|3.6593183|.0952159|202.1188896|4|32/32|
|A3|2.0828435|.0685616|90.4381517|14|14/14|
|B3|1.9980185|.0640454|94.5982259|1|1/1|
|C3|2.0087182|.0596209|126.1366621|2|3/3|
|D3|2.0076912|.0564003|191.1688908|4|32/32|

|组|median s|min/max s|sampleSD s|相对A|
|---|---:|---|---:|---|
|A|90.4381517|84.4823425/92.1165890|4.01186853|基准|
|B|92.1799389|87.7851784/94.5982259|3.45397472|+1.9259%|
|C|126.1366621|119.2810620/128.3257196|4.71870492|+39.4728%|
|D|191.1688908|177.8998640/202.1188896|12.12800304|+111.3808%|

warm A84.6124377/B87.2234695/C118.1423513/D183.5506602s；Bwarm旧proxy，formal统一真实ledger实例包装，不重跑warm，不混统计/继承proofTCP资源。

reserve/consume/settle counts每轮A65/14/65、B67/14/67、C92/20/92、D142/32/142。累计方法spans s（不是wall相加）：
A1 39.1295177/.3274783/39.1417803；A2 42.8259706/.3392311/42.6276799；A3 41.8854086/.3349305/41.9040653。
B1 40.8663369/.3203150/40.7471857；B2 42.1616441/.3481040/43.5439276；B3 44.1260897/.3587369/43.0717441。
C1 56.3326474/.4880317/56.8679095；C2 59.5538648/.5383189/61.4092818；C3 59.9590102/.5995842/59.3737036。
D1 85.5354722/.7608069/86.1369535；D2 96.7278949/.9293454/97.8922058；D3 91.5750194/1.0061546/92.3436920。

ledger lease-accounting observed inflightpeak A33556480/B33818624/C67637248/D135274496B；非RSS/实际分配硬峰。reserve/settle方法累计耗时较高；未单独测fsync/root scan等内部成本，不能进一步归因；减少spawn成立，但本ledger-heavy小负载吞吐退化，不能称提速/基于吞吐默认2或4。执行单没有2×/4×硬gate，负结果送用户审。TCP由server get_request测真实accept；Popen scope无其它子进程调用。D32原因未定位，不推retry。RSS/handles/network/hash/disk/TaskDB/slot/fsync/root scan/queue细分NOT_MEASURED；ledger spans不替代。metadata0不称测metadata性能；loopback合成不外推生产。

## 真实闭环与辅助证据限制

脚本 `C:/Users/PC/AppData/Local/Temp/sakurapool-p5c-bench-4241/real_closure.py`，同cwd命令 `D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe real_closure.py`，job18295d29 exit0。旧批准single-object profile仅内存适配task profile，同origin/repo/worker/token_file ref；不改原profile/不搜替代/不印secret。

pub路径 `D:/SakuraTool/SakuraPool-R2C3-real-20261002/publication`，content digest `23f764d87581fdd2b802e1785947820a5e53647c77a0f9d3cc514299b19a3860`；snapshot `329baa6aa8b8b58677bccd2ef3425ac9bf174aecd5594aaecc7d1791db19f6ae`。网络前fullverify/allowlist/既有first3 extents实核，remaining body8577868422/meta65705450/attempt1841/disk1093353472/records99758/saved996/savedbytes536615620/inflight268435456；原caps不变。剩余值依据本轮既有无网络preflight安全输出 `REMAINING`（limits减status）及 `IDENTITY`，不是重新打开账本追补测量；同命令末尾打印SelectedRecord时发生JSON序列化错误，此前remaining/identity已成功输出，随后只读extent核查修正序列化后通过。

新task `D:/SakuraTool/SakuraPool-P4-work/p5c-real-02cbc98b2de0`；taskid `d735b12300824bd880f0abd24a25e1aa`；plan `ef9acd37ddefc7bed66c84d24c187d686a4b5f1e2c0bc3a6eafcd9f798cc2183`。first3/metadatafalse/maxoutput8MiB。workers1首SETTLED正常requestPAUSE，PAUSED delivered1/error0/unknown0，余2 READY/attemptNULL；partial1/623B。`subprocess.run([sys.executable,__file__,str(task)],check=True)`创建fresh Python process，新connect_profile/PublicationSession，workers2 explicitresume；BOOKKEEPING active峰2是在途future数量，非同时网络证明。COMPLETED3/error0/unknown0，final3/1869B；解析JSON记录partial==final[:1]，seq0/1/2（不是逐字节export前缀）。verify_delivery全成功，attemptcount3。freshproof仅依新session正常verify_conditions路径，无独立trace/PID，不按proofcount delta证明。

后续另一次无网络只读校验实际file SHA==publication mmap expected_image_sha、长度==extent；seq0/1/2 bytes71128/39402/58509全true，总169039B。该pub校验不是real script自身步骤。3个TaskDB attempts与账本请求attempt增量30是不同计量口径。

历史pending before16、pause/final字典相等断言true。脚本把完整old_pending复制到新task `historical-pending-check.json`供fresh子进程读取；该本地受控文件保留、不上传/不作附件、不印内容，不能声称仅存bool或未复制pending。此辅助write_text没有显式ledger计费证据，diskdelta只是账本计量变化，不宣称全物理增量/辅助文件已完整计费；不追补伪计费、不退款、不删旧账本。

|账本|before|after|
|---|---:|---:|
|attempts|159|189|
|totalbody含历史reservation|12066170|12549580|
|metadata|1403414|1716464|
|disk|3201613824|3201843200|
|records|242|242|
|saved samples|4|7|
|saved bytes|255292|424331|
|inflight|0|0|

globaldelta不替代operation receipts；diskdelta229376有上述辅助写入证据缺口。真实验收只该publication/配置/3record，metadatafalse未证明本轮真实metadata输出；旧tasks/pending未修改/退款。

## 复现与停止

有限性能/真实证据已原sole采纳；最终文档有限核通过（含口径更正）；阶段仍WAITING_REVIEW。必要唯一复现脚本 `bench_instrumented.py`、`bench_matrix.py`、`real_closure.py`暂保于 `C:/Users/PC/AppData/Local/Temp/sakurapool-p5c-bench-4241`；直接依赖keeper env/current source-tests/BASE archive source-tests/BASE独建worker/current worker均先保，不删依赖后虚称可复现。不新增hash清单/重复日志；原始短输出job819951ab/18295d29。临时owned artifacts仅确认无后续用途才回收，不删历史证据/未知文件/旧worktrees或refs。

MAIN_MODIFIED=NO；INDEX_MACHINE_OPERATIONS=0；INDEX_BUILD_SHA_CHANGED=NO；P5_D_STARTED=NO。无大规模任务/TAR rescan，未削弱凭证/DNS/TLS/身份/格式/proof/准入/fsync/retry/UNKNOWN。结束停WAITING_REVIEW，不自动main或D。
