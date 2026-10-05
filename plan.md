# P6-A 集中补齐执行路线

当前授权：BASE `2d196f10b7747b3c34429e2de644bf5b08dbfff6`；MAIN `62f86cc487af41de7d84bfb5a14d2fb1e0e172ac` 不动；INDEX_BUILD_SHA `57864e5dd456251b457238b196e8fed5668d909b` 不动。仅 dev、普通显式 stage/commit/push。此为本轮完整执行路线，不是新增准备阶段。

## 唯一写入方与现场保护

- swe 是本轮唯一源码/测试/文档写入方，不委派 read-write 子代理；其他协作均只读。
- 所有操作明确 `D:/SakuraTool/SakuraPool-p5a-clean` 的 cwd/src/manifest；不在旧树构建共享 worker，不杀未知进程。
- `uv.lock` 保留 untracked，不 stage/delete。旧树事故现场只读，不猜测恢复。
- 旧 `D:/SakuraTool/SakuraPool-P6A 验收 #/workspace/tasks/metadata-three` 禁止 resume/reset/refund/receipt 手改；partial2、UNKNOWN1、pending2 保留。新成功不追认旧恢复。

## 执行步骤

- [x] fetch 与 HEAD/origin/dev/main、tracked/staged/untracked 状态核；读取 README、pyproject、agents、PROJECT_RULES、通用规则。TRACKED_CLEAN=YES；UNTRACKED_PRESERVED=uv.lock。
- [x] 梳理现有 worker resolver、启动 capability 与测试节点根因；统一 SAKURAPOOL_RUST_WORKER 权威入口，STREAM/EXPECTED 兼容别名仅集中一次一致性校验，不新增启动框架。
- [x] 统一 lazy chunk 计划与 warm 预测：image/metadata 段数/长度、generation credit/rotation/reproof；预测不替 reserve/proof，同内容身份不得跨代拼接。
- [x] 保留外层 operation accounting，增加底层安全白名单 diagnostic 与 member/chunk 定位；task 串行/pipeline 不丢诊断，metadata 错误独立 fixedcode，禁止泄露 URL/secret/rawexception。
- [x] 整理原36failed/252errors已知文件根因及完整覆盖（exactnode失落明确不推核销）；543pass1skip1deselected consolidatedexit0；只补未成功与本改直接消费者、同 workspace 跨进程 chunk/accounting/recovery，合并选定节点形成可复核成功命令，不轮次相加，不机械全矩阵。
- [x] 本轮Rust源码未变无需重新build；必要受影响 Rust（仅源码实际变才 build/test）；wheel 仅来源及代表 workspace，不再全310/Linux/wheel/stress矩阵。
- [ ] REAL_METADATA_CLOSURE_NOT_COMPLETE：正式新metadata-followup create0/run2 provider_tree_request400CONFIRMEDNOTPUBLISHED；停止第二请求/盲resume，仅一次失败。代码与离线验证后，在同一既有 workspace 建明确新验收 task，沿批准 Publication/revision/credentialref，保旧 pending 计入预算。正式 CLI metadata run/pause/partial/fresh resume/final，遇确定400先定位请求构造，新 UNKNOWN 即停保。
- [x] 核冻结 P2 与明确 remote-map、正式 adapter.source/dataset_id；复用 combine/compile/builder，生成 Danbooru 与 Konachan 各自完整独立 Publication。新目录不改旧 merged/P2，不 TAR 扫描/图像下载/索引机操作；原 ID/extent 保持、新 RID/数组重编且新 snapshot/publication 身份独立。
- [x] 流式对照 source 包与 merged source-filter recordID/必要字段/代表 query/location，正式 full verify；测实际 uncompressed 总量/components/bytes-per-record、Danbooru catalog 表/索引占比，不按比例估算，不再 gzip/Zstd。
- [ ] core已提交push 1a612e396a608bd6ba391a8955c370e19213dee7/tree3577915b55aa47fa8dd6a37866e8945c7ab4f98b，origin/dev核一致main不动；sourcepack独立候选待最终事实核后普通commit/push，报告随实现不另evidencecommit。
- [ ] 最终FOLLOWUP完整事实报告已形成，真实闭环未完明确VALIDATION_PARTIAL；source提交后正文交付finalSHA/两提交/远端状态，停止P6_AWAITING_REVIEW，不下一阶段。

## 新授权优先

本轮明确允许同一 workspace 的新验收 task、从冻结 P2 编译来源独立包；上轮禁止替任务洗 UNKNOWN/禁止索引重建不被误读为拒绝本轮这些新授权。LICENSE/metadata/releaseCI/visibility不改；历史报告保持原事实，新报告不洗旧失败。
