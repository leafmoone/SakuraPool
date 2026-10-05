# P6-A ORIGIN COMPATIBILITY FINAL CLOSURE

## 当前 Origin 收口：P6_AWAITINGREVIEW / VALIDATION_PARTIAL

|最终字段|实际结论|
|---|---|
|PROVIDER_ORIGIN_DIFFERENTIAL|CURRENT_PROFILE_ORIGIN_ABC302；历史400未复现、原因未确定|
|SDK_ORIGIN_RESULT|Origin302→一个redirect destination206，实际read1byte后close|
|RUST_EQUIVALENT_RESULT|正式同workspace独立conditional probe PASS，非任务交付|
|REQUIRED_PUBLIC_HEADER_DELTA|NONE_OBSERVED|
|ORIGIN_RANGE_REQUIRED|UNKNOWN|
|PRODUCT_FIX|NONE|
|REAL_FINAL_WORKSPACE_TASK|BLOCKED|
|REAL_DELIVERED / REAL_UNKNOWN|新final0/3 / operationUNKNOWN1、itemUNKNOWN1、attemptUNKNOWN1（同一失败不同层，不加总）|
|WORKSPACE_CORE / CONFIGURABLE_CAPACITY / CHUNKED_LARGE_MEMBER / SAFE_DIAGNOSTICS|既审范围PASS；不冒本次真实闭环PASS|
|Danbooru / Konachan|PASS_OFFLINE；5196037records/1444122664bytes、33377records/11860113bytes，冻结不重测|
|MAIN_MODIFIED / INDEX_MACHINE_OPERATIONS / NEXT_STAGE_STARTED|NO / 0 / NO|

### 用户允许后唯一新final任务及finally只读审计

用户明确允许现有当前兼容性证据后新final task，不降revision/身份门、不制造产品修复。原两个UNKNOWN永久不动。创建前实际HEAD1d88cfedf5fe89d4da1ac837d3c040697557dd95，dirty仅本轮FOLLOWUP与既有??uv.lock，无活job；workspace正式inspect exit0：policyversion1/capversion1原disk4GiB/inflight256MiB，historicalpending4/inflight0，usageattempts67 body2637164 metadata1206556 disk2719744 saved_samples2 saved_bytes515084。

所有命令cwd D:/SakuraTool/SakuraPool-p5a-clean；PY=D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe，PYTHONPATH同src，TMP/TEMP/TMPDIR指D:/SakuraTool/SakuraPool-P6A-origin-temp，run加SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe。
```text
PY -m sakurapool task create --workspace "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace" --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --query "D:/SakuraTool/SakuraPool-P6A 验收 #/query.json" --selection records --records "D:/SakuraTool/SakuraPool-P6A 验收 #/records.json" --metadata --task-dir "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-origin-final"
PY -m sakurapool task run "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-origin-final" --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --workers 1
```
实际create bash257479b9 exit0，唯一新task2ee1f9ef2dec435fb99f57b6c4bceb4a；metadata=true、3records来自原records.json：bf0d3acda977634327904e3a01308469、98117af5261583921f9b3eb65f58a051、572fe3c5e783359c9d5d73ab1bd141f4，query{}。plan_digest9d5a5f3522e2d73efdf8b2504c4c8468d2603017d78e51f8792fb6a9fbc21d82，publication_digestc186f0a93d9bcf036f54a926ddae3b43aef35d6cfac4eb6dae2151abebcbd0eb，snapshot226a63935e73dc3d628024615425ad9d75b985686646d15dec151aec205feede，selection_digest2b6bfc24cbcab6f8468de0b08ae705b43314eec284f1e6f2cea12f4aeb990f19。正常create返回READY3/UNKNOWN0；无改selection/pin/profile/容量。

唯一run bash8889d101 exit2，自然返回安全诊断：accountingUNKNOWN、accounting_scopeOPERATION、cause_accountingUNKNOWN、cause_codeorigin_transport、cause_phaseorigin、chunk_index0、member_kindimage、cleanupSAFE、codepublication_range、deliveryNOT_PUBLISHED、output_leaseCONFIRMED、phasepublication_fetch、recoverablefalse、secondary[]。本次为origin_transport，无http_status，不能沿历史400补HTTP400原因。立即停止全部该task网络，不resume、不重建替任务、不退款、不清pending、不再probe；无pause/partial/finalexport及freshresume，均NOT_COMPLETED，不能以SAFEcleanup/outputleaseCONFIRMED洗operationUNKNOWN或声称交付。

finally正式task/workspace inspect exit0：新taskBLOCKED、delivered_confirmed0/3、error_count1、unknown_accounting_count1；items BLOCKED/accountingUNKNOWN1、READY/NONE2。TaskDB Path.as_uri()+mode=ro安全固定列投影：唯一attempt phaseOUTPUT_RESERVED/network_stateUNKNOWN/deliveryNONE/accountingUNKNOWN、codeNULL、receipt不存在、outputlease引用存在；不输出任何原receipt/URL/secret。output目录普通文件0。outeroperationUNKNOWN、itemUNKNOWN1、attemptUNKNOWN1是同一失败不同层，不合计为3个操作。

权威前后（before确存于guard-probe-final-result.json并有创建前workspaceinspect，after为finallyworkspaceinspect）：pending_count4→6，是历史4+本次新增2的数量观测，不替逐lease消费确认；inflight0→0。usageattempts67→81(+14)，body2637164→3119045(+481881)，metadata1206556→1522449(+315893)，disk2719744→2760704(+40960)，saved_samples2→2、saved_bytes515084→515084、records0→0。usage含pending reservation，绝不把上述delta全部称实际consumed；saved2只指旧交付，本次新增0。两个旧UNKNOWN现场未操作，新final亦禁止恢复，除非另有可信durable settlement及单独恢复决策。

最终仅本报告与plan的文档收口，不产品/Rust/测试改动，不full、不source再编译；SDK/Rust兼容通过不覆盖真实失败。安全raw最小结果必要证据保留，不新增hash清单；专用环境/cache/scripts回收只触本轮闲置普通自产文件，真实6pending资产不碰。审结后普通单docs语义commit/pushdev，最终SHA/tree及远端核写最终正文，停止P6_AWAITINGREVIEW。

### Origin定位证据（以下不是实际任务成功）

本轮基线1d88cfedf5fe89d4da1ac837d3c040697557dd95/treeee20b562ad4a9781a0f4b1860568e74c467b4621，fetch正式exit0 dev/HEAD/remote一致，main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac，trackedclean仅??uv.lock；无活projectPython/worker。唯一写方swe，两个UNKNOWN task永久保留未操作，四pending不清。无full/Rustbuild/sourcecompile或全量再验。

### 固定Origin最小矩阵与官方stream

本轮D:/SakuraTool/SakuraPool-P6A-origin-temp/sdk-env隔离重装，PyPI官方ModelScope1.40.1、modelscope-hub0.4.5、Python3.13.15；安装bash4f71847f exit0，无keeper依赖改变。token仍仅进程内读取批准credentialref，未login/save_token；日志disable，禁止rawURL/Location/header秘密值/body输出。诊断明确不冒任务账务。

实际profile.origin=https://modelscope.cn，Rust代码fixedpath/api/v1/datasets/leafmoone/webdataset_danbooru_v3/repo，query Revision=73306f1dc5459238710f477b376c36da997d020c/FilePath=anime_pictures/t0992-001.tar。第一矩阵误用SDK默认www.modelscope.cn，bash498c4fa6 exit0虽ABC均302，但撤回其Rust-equivalent标签，仅默认host背景证据，不覆盖原Origin。随后profilehost矩阵bash95c525a5 exit0：A实际Accept application/json,application/octet-stream、UserAgent SakuraMoon/1、identity、Rangebytes0-0、Bearer/cookiepresence true；B加Content-Typeapplication/json、freshX-Request-ID；C用官方DownloadManager._build_download_headers实际UserAgent及实际download构造snapshot-identifier。可选基础设施region探测显式disable，regionheader absent，非猜region。A/B/C均302，Location/ContentLengthpresence true、ETagpresence false，bodyread0，streamclose，不follow。ABC非全400，D未执行；B未改善失败，不发单项header试错。fixed preparedscheme/host/path/query在执行后离线assert一致（不冒执行前校验）。

官方LegacyClient.download_stream源码实际stream=True，但_request错误会读resp.text、requests.resolve_redirects默认drain resp.content，故先SDK Session.send有界stop捕获Origin302，不冒完整SDK结果。后授权最小官方stream，实际download_stream原redirect流程，HTTPAdapter关闭redirectbody且置已消费empty，防无界drain，并在任何crossorigin send前移除Authorization/Cookie；maxredirect2，禁非https/userinfo，最终只rawread1byteclose。bash075206b6 exit0：origin302/authcookie true→一个redirectdestination206/authcookie false，最终status206实际read1byte，没有整TAR落盘、背景prefetch或保存signedURL。记录只阶段/跳数/status/presence，不以206证明整图片SHA或task交付。

### 实际Rust正式workspace独立probe

只读冻结mergedpublication catalog匹配批准object object_size=0x505dc000；仅该对象小范围probe，不扫描来源包。实际ProviderObject类型为modelscope_dataset_legacy，originalprofile origin/worker，RustProductionTransport.verify_conditions正式reserve/settle同workspace且计历史pending。最终bash4a8863eb exit0 PASS：actual freshLocation/CDNobserve、positive206同SHA、negative412，strongvalidator/cdnhostpresence true，不输出其值。attempts61→67、usagebody2636723→2637164；metadata1206556、disk2719744、saved_samples2/saved_bytes515084、pending4、inflight0不变。独立probe不重放任何task；saved2仍旧交付。

前置临时脚本错误分开保：bashdfb72a2b exit1 BudgetLedger.inspect不存在，在网络前；bashcba7e406 exit0 caughtValueError为误用SDK datasetenum，权威attempts61/pending4前后完全不变，未发网络；改临时输入后才上述真正probe。不是产品修复，未把失败脚本计实际网络测试。

当前PROVIDER_ORIGIN_DIFFERENTIAL=CURRENT_PROFILE_ORIGIN_ABC302；SDK_ORIGIN_RESULT=302_TO206_READ1；RUST_EQUIVALENT_RESULT=ACTUAL_GUARDED_CONDITIONAL_PASS；REQUIRED_PUBLIC_HEADER_DELTA=NONE_OBSERVED；ORIGIN_RANGE_REQUIRED=UNKNOWN（带Range成功仅证允许，不证必需）；PRODUCT_FIX=NONE；historical400仍NOT_REPRODUCED_CAUSE_UNDETERMINED，不称headers已修或外部永久故障。该诊断时点尚未创建任务；随后用户明确允许新final任务，实际失败及最终账务见本报告最前收口。两个旧UNKNOWN不恢复。

---

## 历史：上一 FINAL CLOSURE（1d88cfedf5fe89d4da1ac837d3c040697557dd95）

## 当前最终收口（以下历史补齐报告不覆盖此节）

本次基线9aebcb74311d4e598ebf31e3e0f01873c91b20b8/treea2f73ed325a37eef830e6c1cab39b608e7679192。唯一写方swe；产品源码未改，仅4tests旧fixture契约修及文档。FINAL_COMMIT由最终正文给出，报告不自含自身SHA；main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac不动。

|最终字段|实际结论|
|---|---|
|状态|P6_AWAITINGREVIEW / VALIDATION_PARTIAL|
|REGULAR_REGRESSION|FAILED_ENVIRONMENT_AND_FIXTURES；不冒一轮fullPASS|
|TARGETED_RECOVERY|108PASS2SKIP + 1PASS，分两轮，非109同轮|
|PROVIDER_DIFFERENTIAL|SDK_AND_GUARD_TREE_PASS / HISTORICAL400_UNDETERMINED；旧400 NOT_REPRODUCED_CAUSE_UNDETERMINED|
|REAL_METADATA_CLOSURE|BLOCKED：同task授权resume发生origin400/operationUNKNOWN，立即停保|
|DELIVERED / UNKNOWN（新task）|0/3 / 1|
|WORKSPACE_CORE|PASS_OFFLINE（现有selected及本次定向消费者）|
|CONFIGURABLE_CAPACITY|PASS_OFFLINE（现有native边界及定向容量消费者）|
|CHUNKED_LARGE_MEMBER|PASS_OFFLINE（现有chunk/generation/reproof验收，不冒真实任务成功）|
|SAFE_DIAGNOSTICS|PASS：安全CLI/TaskError透传，TaskDB仅fixedcode不完整cause持久|
|Danbooru / Konachan|PASS_OFFLINE，5196037records/1444122664bytes；33377records/11860113bytes|
|OLD_UNKNOWN_TASK_MODIFIED|NO|
|LEGACY_LEDGER_MIGRATED|NO|
|MAIN_MODIFIED|NO|
|INDEX_MACHINE_OPERATIONS|0|
|NEXT_STAGE_STARTED|NO|

### 一次full与精确受影响复测

授权唯一full命令：cwd D:/SakuraTool/SakuraPool-p5a-clean，PYTHONPATH同src，SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe；PY=D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe。
```text
PY -m pytest tests -q -m 'not stress' -p no:cacheprovider --tb=short -rs
```
整个full产品/tests冻结于上述基线tree；bash33d3c9f8 timeout0，exit1：66 failed, 1650 passed, 6 skipped, 1 deselected, 45 errors in3339.92s(55:39)。逐111trace核：49fail+45setupERROR直接NoSpace/dbdiskfull；17独立assert旧fixture期待不归环境。C盘首实际Free0，D210457436160B，full temp实际pytest-1169。

6skip逐node原因：test_local_partition_builder::test_lock_symlink_is_rejected symlink权限；test_p4_package symlink参数分支权限；test_p5b_prerequisites Windows literal ?路径分支、symlink权限分支；test_query_batch_compat nativegetlimit缺失运行时条件；test_runtime_remediation symlink权限分支。单deselect为notstress排除stress项，不宣其通过。完整原shortsummary在临时regular.stdout核后必要统计保此，不计算hash。

仅本轮pytest-1169普通自产synthetic643231388B安全回收，root非reparse、无活pytest/worker、无lock；两reparse及ancestor保留，不跟target，不清父/未知历史。回收即时C free277766144→931971072；此前Free0到即时值自主变化不全归删除。后续全部TMP/TEMP/TMPDIR及basetemp/cache指向本轮Downed。

17最小tests修：bounded_json三项code改publication_metadata，validationfailed/外UNKNOWN/pending/秘密负例不删；p5c_c1六失败及同公式断言用(32<<20)+protocolmemory(transport.capacity)，不写新魔数，峰值payload+2/+14保持exact且旧仅32MiB会失败；publication八exactdict新增固定image/chunk0及image_settle固定remote_io/transport/UNKNOWNcause，不放宽subset、不忽略未知字段，旧accounting/cleanup/secretassert保。

受影响集合逐node保reports/P6A/final-closure-affected-nodes.txt（111unique，由full每个traceheader+首tests文件提取，49+45+17对账）。命令：
```text
PY -m pytest <该文件每行node作为独立argv，按原顺序> -q -p no:cacheprovider --tb=short -rs --basetemp=D:/SakuraTool/SakuraPool-P6A-finalclosure-temp/affected-tests
```
实际bash1555c605 exit1：108 passed,2 skipped,1 failed in906.84s。两skip精确test_task_safety.py::test_linked_task_journal_rejected、test_workspace.py::test_link_task_path_rejected，均symlink权限。唯一fail为test_task_workspace_workflow.py::test_local_workspace_run_resume_and_unknown_network：假LocalTransport未super初始化却继承nativepredict，_lane_lock缺失；检索该类唯一定义在test文件非产品子类。仅fixture显式predict_warmFalse走coldproof，proof/range/receipt/预算assert保。单该node相同环境新的workflow-test basetemp复测exit0：1 passed in1.41s。不能把108+1冒109同轮。前启动shell转义SyntaxError发生pytest前exit1，非测试轮。4testRuff最终exit0（先行长行3项修后0），产品未改，不第二full/111。

### 官方SDK/guard A→B→C→D与同taskresume

本轮授权官方SDK例外A/B直接调用不冒guard预算结果。用户批准安装，仅D短期sdk-env Python3.13.15，uv --index-url https://pypi.org/simple，实际ModelScope1.40.1/modelscope-hub0.4.5，import D:/SakuraTool/SakuraPool-P6A-finalclosure-temp/sdk-env/Lib/site-packages/modelscope/__init__.py；未改keeper。官方HubApi内存token+legacy内存cookie，不login/save_token，maxretries0/trust_envFalse/logdisable，不打印rawexception/body/token/cookie或signedURL，不搜其它凭证。credential_ref仍D:/sm_data/ms-token.tmp仅进程内读。

固定repo leafmoone/webdataset_danbooru_v3、numeric218254、rev73306f1dc5459238710f477b376c36da997d020c、Rootanime_pictures、RecursiveTrue、PageNumber1 PageSize20，没有参数循环/Range图片。SDK A get_dataset_id_and_type实际repoGET200 id218254/type4，B get_dataset_files固定treeGET200/20entries，bash5515f853exit0。A证明该配置repo访问可用，不以公共repo200冒用户身份专门校验。

C keeper connect_profile.metadata_control→ModelScopeDataset.legacy_hub_id/legacy_tree_page，同既有ws正式reserve/settle，bash6fe12afaexit0，repo/tree200/20entries。usageattempts51→53、body2311884→2320828、metadata881719→890663、pending2/inflight0/saved2不变；body字段含历史pending不全部consumed。

D实际methodGET/path/api/v1/datasets/218254/repo/tree与query所有key/value一致；SDKheadernames Accept,Accept-Encoding,Authorization,Connection,Content-Type,Cookie,User-Agent,X-Request-ID；guard缺Content-Type/X-Request-ID其余名称同，两侧credentialpresence/cookiepresence均true。未保秘密header值/额外hash。当前皆成功，无证据把header差异当旧400产品因果；不改下载代码、不称外部当前拒绝请求。

在tree实际guard200后，只读existing metadata-followup task仍BLOCKED0/3UNKNOWN0首CONFIRMEDREADY3无output，正式freshprocessexplicitresume sameba84a6c5e2b84f7f8eda488a9c75f1be，不建第三task：
```text
PY -m sakurapool task resume "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-followup" --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --workers 1
```
实际bash0ed9b17e exit2安全诊断：accountingUNKNOWN、coderejected、deliveryNOT_PUBLISHED、http_status400、phaseorigin、recoverablefalse、secondary[]。立即停止所有该task网络，不再resume/reset/refund/replay/export冒成功；tree200与origin400是不同阶段不归同因。最终只读inspectexit0：BLOCKED，delivered0/3，unknown1，itemBLOCKEDUNKNOWN1 READY2；workspacepending4(旧2+新增2)、inflight0，attempts61 body2636723 metadata1206556 disk2719744 saved_samples2 saved_bytes515084。新任务也保UNKNOWN现场，今后禁止自动/显式重放、reset/refund/改receipt；除非有完整可信durable settlement与单独恢复决策不得恢复。pending4是数量观测，旧2+新增2不替逐lease消费确认；saved_samples2指旧交付，非新task交付。绝不以底层CONFIRMED洗外UNKNOWN。旧metadata-three永只读原partial2UNKNOWN1，未操作。

本次真实闭环未完成，无finalexport；既有pause/partial证据不追认此次成功。source两包冻结未compile/全量对照/压缩或上传；既有全components/DBSTAT见下述来源节。普通提交最终SHA/远端核在最终正文，完成后停止送审，不下一阶段。

---

## 历史：集中补齐交付（9aebcb7，以下任务UNKNOWN0/未resume描述仅当时事实）

BASE2d196f10b7747b3c34429e2de644bf5b08dbfff6；main62f86cc487af41de7d84bfb5a14d2fb1e0e172ac不改。源码唯一写入方swe，其他协作只读。两个独立语义提交：core15paths保持已验证审查树，sourcepack包含两个离线脚本和本完整报告/历史入口/执行单。core提交1a612e396a608bd6ba391a8955c370e19213dee7（tree3577915b55aa47fa8dd6a37866e8945c7ab4f98b）已普通push origin/dev并ls-remote精确一致，main仍62f86cc487af41de7d84bfb5a14d2fb1e0e172ac；source提交SHA最终由正文交付回执给出（本报告作为该commit内容不能自含自身SHA，不另evidencecommit）。

## 范围、契约与安全边界

- 权威测试worker resolver集中SAKURAPOOL_RUST_WORKER；STREAM/EXPECTED为兼容别名一致性检查，不独立选择不同binary。使用D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe；当前Rust源码与BASE一致，未本轮重build，路径相等不被用作源码provenance。
- StreamPlan lazy arithmetic，warm hint不发coordinator ledger RPC；真实generation refresh policy/reserve/reproof仍权威，rotation不换object/revision/validator拼内容。
- serial真实nonprepared调用通过PreparedFetch factory取得权威identity，pipeline原SQLtuple正确不是旧bug；共用helper使消费者一致。
- 诊断采用固定枚举与boundedchunk整数，不暴露URL/Location/token/cookie/rawexception；底层CONFIRMED不提升外层UNKNOWN。metadata验证独立固定code。CLI即时完整诊断不冒TaskDB现fixedcode字段持久细节。
- 不新增public格式/SDK启动框架、不改workspace capacity/policy数值、旧receipt/ledger不重写。两包仅本轮指定Danbooru/Konachan批准P2子集；其余三source未compile，不承诺全上游完整。
- 所有新core源码仅15paths；source脚本是离线管理员工具，复用现inventory/compiler/builder。没有生产大规模下载、TAR重扫、索引机操作、LICENSE/发布metadata/CI/visibility改动。

## 原失败覆盖口径

原常规tree7dcd890ffa3cf8da54b415f1850b71ff6a46f10d：36failed1435passed36skipped1deselected252errors2174.31s exit1。完整shortsummary无法从当前会话/仓库恢复，root亦无保存位置，不宣逐一核销36/252 exactnodes。

明确已知文件范围：Raw test_p4_transport/test_p4_tar_faults/test_p4_package/test_empty_package/test_staged_lookup，RPC/lane/control专项曾108pass1skip；worker缺失组test_r2_production/test_r2c2_binding/test_r2c2_canary_admission/test_r2c2_configured/test_r2c2_identification/test_r2c2_negative_body/test_r1_bridge/test_r1_bridge_manifest/test_r1_gate_e_e2e/test_r1_loopback_loop/test_r1_worker_attempts曾229pass1fail(旧handshakecapability后来通过)；contract test_core_capacity_layout/test_r2c1_resources/test_p5c_c0/test_task_metadata曾serial所有成功但同调用2新rawfixturefail exit1。当前完整文件成功仅证明文件覆盖，不补造失落nodes。

## 本轮命令结果

pytest cwd固定D:/SakuraTool/SakuraPool-p5a-clean，PYTHONPATH其src，主SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe；Rust源码未变未build。

- streamcapacity非actual_rust4pass2deselected0.46s exit0（不是native证据）。
- streamcapacity/protocolmemory/headerboundary23pass5.82s exit0，统一主resolverenv，无STREAM需。
- followupcore/streamdelivery/taskmetadata18pass31.11s exit0；其后同shellruffimportsort exit1，修后全ruff0。
- followupcore5pass1.39s exit0：lazy多chunk、credit边界、实际_admit_generation旋转、两个freshprocess同syntheticworkspace消费/settle/pending保持。初fake lane漏_rotating1fail4pass修fixture。
- bash596373ca selected120s timeout exit-1，无统计；期间修改共用导入文件，混合现场不可绑定单tree，不作通过。进程核无旧command残留，未扫杀。
- serialpreflight新增API误取task.endpoint/metadata、遗漏max_chunk的失败保留。修后bash6c947a8c17pass115.36s exit0；只读核发现locator索引/nonpreparedSQL仍需后续修，现fixture未证明binding。
- bash2f856421：pytest -q tests/test_p6_followup_core.py tests/test_task_resources.py tests/test_stream_delivery.py tests/test_task_metadata.py tests/test_p5c_c2.py --tb=short -p no:cacheprovider，timeout900；最终timeout exit-1，无完整统计，后文保定位与修后证据。

## 修后直接证据与选定冻结

bash2f856421最终900s timeout exit-1，仅partialprogress，无完整统计，非通过。其后actualimport sys.modules核compile/inventory/publication入口无tasks/production/prepared_fetch/publication_fetch，允许core修改不漂sourcebuild。

serial非prepared路径曾新增不存在对象SQL列及错locator索引，修为现PreparedFetch._prepare权威boundedprojection，共享transport_identity显式(endpoint,repo,repo_type,revision,path,size)。pipeline原tuple正确，仅复用helper；此前误报旧pipeline错位已撤销，不记为产品旧bug。

nativewarm快测1failed2passed138.60s：predict_warm在coordinator调用generationbudget→ledger.status RPC禁用。修predict-only refresh=False，真实admission仍freshpolicy/reserve；55520c66最终8passed159.04s exit0（followup5、serial两种binding一致1、nativewarm/stale2）。新增policy收紧hint仍warm但fresh真实wsreserve拒、attemptcredit消费后cold，followup6passed1.48s；Ruff/diff0。

实际冻结core/test tree `3577915b55aa47fa8dd6a37866e8945c7ab4f98b`（15显式stagedpaths，完整tree包含基线；文档及source脚本另分包，后续报告改动不改变此执行源码）。git write-tree/cachedcheck exit0，uv.lock未stage。

当前合并选定bash12fefe38 timeout0：known28files及直接consumer完整范围，-m notstress；非第二整套矩阵，不按轮次相加。命令显式cwd/src/currentworker，所有其导入产品/tests冻结；正式最终exit0，543 passed, 1 skipped, 1 deselected in 2676.59s (0:44:36)。覆盖原明确knownfiles不是失落36/252逐node核销。随后在该执行源码上进行了唯一一次新真实task请求；失败情况见后节，不与543离线通过混同。单skip由已知文件test_p4_package的symlink privilege分支可解释，但无-rs最终逐node回执，不能宣exact skipped node已恢复；单deselection对应唯一stress标记test_p5c_c2::test_real_request_threshold_rotates_and_reprobes（由源码定位与not stress命令证实），未跑stress不宣其通过。

### 合并选定的精确范围（28个文件）

```text
cwd D:/SakuraTool/SakuraPool-p5a-clean
PYTHONPATH=D:/SakuraTool/SakuraPool-p5a-clean/src
SAKURAPOOL_RUST_WORKER=D:/SakuraTool/SakuraPool-p6-stream-target/debug/sakurapool-worker.exe
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -m pytest -q tests/test_p6_followup_core.py tests/test_task_resources.py tests/test_stream_delivery.py tests/test_task_metadata.py tests/test_p5c_c2.py tests/test_core_capacity_layout.py tests/test_r2c1_resources.py tests/test_p5c_c0.py tests/test_p4_transport.py tests/test_p4_tar_faults.py tests/test_p4_package.py tests/test_empty_package.py tests/test_staged_lookup.py tests/test_r2_production.py tests/test_r2c2_binding.py tests/test_r2c2_canary_admission.py tests/test_r2c2_configured.py tests/test_r2c2_identification.py tests/test_r2c2_negative_body.py tests/test_r1_bridge.py tests/test_r1_bridge_manifest.py tests/test_r1_gate_e_e2e.py tests/test_r1_loopback_loop.py tests/test_r1_worker_attempts.py tests/test_stream_capacity.py tests/test_protocol_memory.py tests/test_header_boundary.py tests/test_raw_stream_workspace.py -m 'not stress' --tb=short -p no:cacheprovider
```

|根因/直接消费者|完整文件覆盖（不逐原失落node推定）|
|---|---|
|本轮新增边界、policy、跨进程|test_p6_followup_core|
|serial/pipeline preflight、RPC/generation/lifecycle|test_task_resources,test_p5c_c2|
|stream chunk/pin/reproof及metadata独立diagnostic|test_stream_delivery,test_task_metadata|
|capacity/header/footprint旧契约|test_core_capacity_layout,test_r2c1_resources,test_p5c_c0|
|Raw消费者与package cascade|test_p4_transport,test_p4_tar_faults,test_p4_package,test_empty_package,test_staged_lookup,test_raw_stream_workspace|
|R2原缺worker setup|test_r2_production,test_r2c2_binding,test_r2c2_canary_admission,test_r2c2_configured,test_r2c2_identification,test_r2c2_negative_body|
|R1原缺worker skips与旧capability|test_r1_bridge,test_r1_bridge_manifest,test_r1_gate_e_e2e,test_r1_loopback_loop,test_r1_worker_attempts|
|统一resolver/capability容量native|test_stream_capacity,test_protocol_memory,test_header_boundary|

## 来源包与现场

原approved资产D:/SakuraTool/SakuraPool-P5D-20261004T143905Z两rootlist实际集合相等exit0。INPUT.adapter.source/dataset正式分类Danbooru181roots/Konachan4roots，仅批准冻结子集非全上游。

bash258eec02正式exit0：reports/P6A/build_source_publications.py --asset上述资产 --output D:/SakuraTool/SakuraPool-P6A-source-packages（timeout0）。新owned目录exist_ok=False；source独立remote-map exactcoverage，combine/compile/build/fullverify；compiler/inventory/publication及脚本启动期冻结，不改旧merged/P2/remote-map、不TARscan/image下载。builder load_publication(full_verify=True)成功：Danbooru5792objects/5196037records、Konachan104/33377，全fetchable；manifest publication_id与snapshot_id字段实际分别相等（按builder契约），Danbooru两字段均b02fbf37dac10487f4c22dcaa452d7471b41ef1ef96bf8b6b39be6570b3b250e，Konachan两字段均a6fbf5ca11b2ffd2adab9fb5c8be931499bdb5f9551570cccd6b6445210e3f5b；这些不是Publication.content_digest。builderfullverify后READY记录manifest content_digest分别8850e1a98020098c669a761938707acf6ee6706e50d62b5b59608503f0cce08d、79c8851b3771f17cbb5bd794efc904454c8abbee1a6eeb4a5107fdbedc15d012。全fetchable仅结构覆盖，不是新实际下载证据。manifest runtime_bytes1275312670/10725139、image_sha256_bytes166273312/1068192、remote_objects_bytes2535424/65536。bash437b0f40正式exit0：source与mergedsource过滤完整流式逐项相等（5196037/33377条recordID/source/dataset/post、对象ID/ref、remote identity、extent、format、imageSHA）。Canonical RID排序分别定位，RID数字不跨snapshot比，重复post不去重；每source top3tag all/none六query membership逐recordID相等。此为完整冻结子集offline验收非下载认证。

实际文件stat total含控制文件：Danbooru1444122664 bytes，277.9277099066077 B/record；Konachan11860113 bytes，355.33789735446567 B/record。Danbooru components catalog623214592/bitmaps449449984/locations202645781/imageSHA166273312/remote2535424，其余控制文件3571；runtime_bytes已含runtimecomponents不重复相加。Konachan catalog5779456/bitmaps3641344/locations1302041/imageSHA1068192/remote65536，另控制文件3544。DBSTAT Danboorucatalog records186966016 bytes(30.000262894999735%)、record_id unique index144343040(23.161049476838952%)、source_post与dataset_post各97882112(15.706004521793995%各)，表与索引其余实测条目在verification.json及下述完整表。不按记录比例估算/不用压缩bytes替代安装。

### 全部安装组件（bytes，snapshot内文件按basename列出）

|组件|Danbooru|Konachan|
|---|---:|---:|
|catalog.sqlite|623214592|5779456|
|bitmaps.sqlite|449449984|3641344|
|locations.npy|202645781|1302041|
|image_sha256.npy|166273312|1068192|
|remote_objects.sqlite|2535424|65536|
|PUBLICATION.json|1194|1182|
|Publication READY|64|64|
|runtime/current.json|240|240|
|snapshot OWNER.json|488|488|
|snapshot READY|64|64|
|SNAPSHOT.json|1516|1501|
|STAGE.txt|5|5|
|合计|1444122664|11860113|

DBSTAT实分配量，百分比分母分别catalog文件623214592/5779456 bytes；全部页合计精确对应catalog大小。

|表/索引|Danbooru bytes|%|Konachan bytes|%|
|---|---:|---:|---:|---:|
|datasets|4096|0.00065724|4096|0.07087172|
|formats|4096|0.00065724|4096|0.07087172|
|meta|4096|0.00065724|4096|0.07087172|
|namespaces|4096|0.00065724|4096|0.07087172|
|objects|2146304|0.34439245|40960|0.70871722|
|records|186966016|30.00026289|1122304|19.41885188|
|records_dataset_post|97882112|15.70600452|540672|9.35506733|
|records_source_post|97882112|15.70600452|540672|9.35506733|
|sources|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_datasets_1|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_formats_1|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_meta_1|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_namespaces_1|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_objects_1|831488|0.13341921|16384|0.28348689|
|sqlite_autoindex_records_1|144343040|23.16104948|897024|15.52090716|
|sqlite_autoindex_sources_1|4096|0.00065724|4096|0.07087172|
|sqlite_autoindex_tag_categories_1|17653760|2.83269362|495616|8.57547838|
|sqlite_autoindex_tags_1|20938752|3.35979810|561152|9.70942594|
|sqlite_schema|4096|0.00065724|4096|0.07087172|
|tag_categories|14790656|2.37328461|421888|7.29978738|
|tag_categories_category_tag|17608704|2.82546401|495616|8.57547838|
|tags|22126592|3.55039697|602112|10.41814316|
|合计|623214592|100|5779456|100|

执行命令（cwd和PYTHONPATH均为本followup树，与selected一致）：
```text
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe reports/P6A/build_source_publications.py --asset D:/SakuraTool/SakuraPool-P5D-20261004T143905Z --output D:/SakuraTool/SakuraPool-P6A-source-packages
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe reports/P6A/verify_source_publications.py --merged D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --output D:/SakuraTool/SakuraPool-P6A-source-packages
```
两命令exit0；管理员产物build-results.json和verification.json位于上述output根，非仓库副本，必要metrics由本报告保留。source脚本执行时既有compiler/inventory/publication源码均与BASE一致，新增脚本保持执行原内容，sole有限核PASS已由root正式路由；不是sole额外独立READYdigest计算。

## 新同-workspace正式CLI验收（当前BLOCKED，非闭环成功）

正式CLI命令（固定cwd D:/SakuraTool/SakuraPool-p5a-clean，PYTHONPATH同src、SAKURAPOOL_RUST_WORKER同selected；profile仅引用不读取/输出credential内容）：
```text
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -m sakurapool task create --workspace "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace" --publication D:/SakuraTool/SakuraPool-P5D-20261004T143905Z/publication --query "D:/SakuraTool/SakuraPool-P6A 验收 #/query.json" --selection records --records "D:/SakuraTool/SakuraPool-P6A 验收 #/records.json" --metadata --task-dir "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-followup"
D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe -m sakurapool task run "D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-followup" --profile "D:/SakuraTool/SakuraPool-P6A 验收 #/profile.json" --workers 1
```

sole通过实际coretree3577915b及source脚本/metrics有限核，由root正式路由许可。task create exit0：新目录tasks/metadata-followup，task_id ba84a6c5e2b84f7f8eda488a9c75f1be，plan_digest28bcb1833187a7091f5b262eb9d0a64abeac361b26a35e449e5d69f9b7747145，metadata=true selection3 approvedrecords；workspace ee03b319474d43d1abc64bfb67ed9d26与lineage5a9a80dd77534cd6801cf1b91da527c4原样。create publication_digest c186f0a93d9bcf036f54a926ddae3b43aef35d6cfac4eb6dae2151abebcbd0eb，snapshot_id226a63935e73dc3d628024615425ad9d75b985686646d15dec151aec205feede。

正式run workers1 bash_c3426e11 exit2：安全CLI诊断 `{ "accounting":"CONFIRMED", "code":"http_status", "delivery":"NOT_PUBLISHED", "http_status":400, "phase":"provider_tree_request", "recoverable":false, "secondary":[] }`。inspect exit0 BLOCKED、delivered0/3、error1、READY3（一个CONFIRMED两个NONE）、UNKNOWN0。尚未进入range故无member_kind/chunk_index或包装cause字段，非bottomCONFIRMED提升outerUNKNOWN。TaskDB items仅持久code=http_status、attemptphaseNETWORK_START accountingCONFIRMED；本轮phase/HTTP数字由正式CLI安全回执证明，不称DB完整持久diagnostic。

未盲resume/未pause或export冒闭环；按确定性400先定位。实际失败首record bf0d3acda977634327904e3a01308469对应anime_pictures/t0992-001.tar，Root=anime_pictures，pin73306f1dc5459238710f477b376c36da997d020c。repo名不能按source分类。只读参照官方SDK v1.34.0 hub/api.py 的list_repo_tree/get_dataset_files，其numericHub/repo/tree与Revision/Root/Recursive字符串True/PageNumber/PageSize形状一致，这不是服务接受证据，400原因仍未证实；不换pin/样本、不跳exactlookup。首readonlysqlite URI未encode #产生no such table exit1，改Path.as_uri()+mode=ro后成功，未写DB。

run前workspace inspect实际pending_count2/inflight0；usage原字段attempts47/body2184217/disk2678784/metadata754052/saved_bytes515084/saved_samples2。这是workspace累计status含pending预留，不标body均consumed。run后workspace inspect exit0 pending_count仍2/inflight0；usage原字段attempts51/body2311884/disk2719744/metadata881719/records0/saved_bytes515084/saved_samples2。无新增saved sample。旧metadata-three只读、partial2/UNKNOWN1/pending2原样。SDKtreequery形状一致不证明旧HTTP400原因，cause仍未知。root最终决定停止无假设诊断/第二网络，本轮收口BLOCKED_PROVIDER_TREE_HTTP400、REAL_METADATA_CLOSURE_NOT_COMPLETE、VALIDATION_PARTIAL。新task只有一次run失败，无pause/partialexport/freshresume/final，不制造闭环；新成功亦不得追认旧UNKNOWN。uv.lock未跟踪保留，旧树5paths保护。main/index/LICENSE/metadata/releaseCI/visibility不动，无gzip/Zstd/下一阶段。
