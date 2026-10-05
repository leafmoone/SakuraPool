# P6-A 集中补齐：VALIDATION_PARTIAL（交付候选）

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
