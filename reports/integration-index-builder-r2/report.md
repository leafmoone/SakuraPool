# INDEX-BUILDER-R2-INTEGRATION — WAITING_REVIEW

本轮仅新worktree `D:/SakuraTool/SakuraPool-index-integration`，新branch `integrate-index-builder-r2-20260930`。主dirty/untracked原样，未stash/清理/纳入。无真实请求/TAR/全库/benchmark/P5。

## Git身份与merge

- INTEGRATION_BASE_DEV：`5ce00082d911ac71e600fd8703fa6b344be9bfab`（fetch后实际origin/dev）。
- CANDIDATE_SHA：`95cdae3566323756bfaf99e507db4279cbfc9e22`，精确匹配；共同祖先 `249dc9bebdc3beed40dd81c6067a7a2f8a814ae3`。
- main/origin/main：`edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`。
- 实现merge SHA：`89c6e5d7eac4fe412e66db175498769f86bdbd13`；实现TREE：`c504efba36c97544fc149c7eabc8f332e2a5e333`。
- parents：`5ce00082d911ac71e600fd8703fa6b344be9bfab`、`95cdae3566323756bfaf99e507db4279cbfc9e22`。
- `git merge --no-ff --no-commit origin/index-builder-prep-20260930`，冲突和验证后一次mergecommit收尾，无squash/rebase/amend。已push-u实际integrationbranch、lsremote核验；dev/main/candidate未更新。
- 唯一后续docs提交只此reports目录，不改认证source。最终INTEGRATION_SHA=SUBMISSION HEAD，INTEGRATION_TREE=该HEAD tree；本报告不嵌自引用SHA，最终回复给实际完整身份与四远端refs。

MERGE_CONFLICTS实际清单：仅 `src/sakurapool/storage/rust_index.py`，JSON bound、writer/lease、数据库hash三处。CLI和README自动merge，无其它冲突；未整文件ours/theirs。

语义union：remote保留FileArchive独立whole/memberSHA审计、1MiB JSON防线、传入stagelease；local真实Rust单遍file scan只offset读必要JSON，仍adapter默认16MiB，无整TAR Pythonbytes/重parse。共用_write_stage schema/stamp/marker和iterator member/image计数，DB hash流式计算，恢复自动merge误丢MAX_JSON常量。local16MiB未缩由静态核查；合成fixture JSON<1MiB，**未实测>1MiB阈值**。

CLI保remote inspect/fetch、productionprofile、scan-remote并加build-partition/manifest；三个help smoke通过。README当前runtime说明v2但保留R2gates；历史reports/R2对base diffempty，v1历史证据原样。其它最小修复仅inventory7lines：manifest declared_size/provider_sha256与INPUT validator交叉一致、拒persistedlocal_path；unknown/null保留但须一致。4项重新签contract hash的矛盾测试证明语义拒绝。

## 当前契约与专项证据

RUNTIME_FORMAT_VERSION=2；RUNTIME_COMPILER=sakurapool-p3-v2；P2_DURABLE_FORMAT=4。tagidentity(namespace,value)不变；tags(tag_id,namespace_id,value,cardinality)+tag_categories(tag_id,category)，无tags.category/TAG_CATEGORY_CONFLICT。CATEGORY_FILTER_QUERY=NOT_IMPLEMENTED_BY_DESIGN。v1 snapshot拒绝；旧partition runtime1仅provenance，有效P2v4能不重扫直接compilev2；新metadata记录2。旧v1R2 PASS不替代本轮v2重验。

配对test A本地已有TAR→真实Rustscan一次→file_scan_stage，B相同bytesR2loopback conditional/fullstreamreader→remote stage：成员path/offset/size/SHA/wholeSHA和payload一致，P2四表objectid/recordid/imageJSONextent/tags/categories/caption/widthheight一致，仅backend/repotype不同且明确本地local/远端modelscope。gamecg_native/sano_toshihide artist+general、zerochan_native/tiger character+general各valuequerycount1，categoriesunion；local location读image/JSON并核SHA。既有nested四category/GIF/AVIF/jsonid/postid同时重验。

danbooru_v3 part000000/000001同adapter/hash接受不相交objects/path，跨object重复source/postid保留2rid、resolveone报歧义，反输入顺序snapshot相同；adapter mismatch/dupobject/samepath/samepathdifferentcontent拒。API接受多个叶inventory；已combined虚拟inventory再combine因root/fingerprint拒，未扩此接口，不影响直接多partcompile。

R2两mode在v2下P2v4→compile→valuequery/categories→publish auditedpackage→load_package→recordid→location/auditextent→RustexactimageJSON，真正新python-I/worker重开同ledger/proof；无隐式scan/Pythonfallback。

## 环境与实际验证

K=`D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe`，普通3.13.15，pins不变；Rust1.98.1 windows-gnu，既有target `D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target`、显式releaseworker。PYTHONPATH unset，同唯一keeper，无第二venv。安装前/正式full前轻量无活跃使用者确认，fixedroot仅swe测试，无忽略消失path。

| 命令/范围 | exit | 实测 |
|---|---:|---|
| K -m pip check（开始/恢复） | 0 | 无坏依赖 |
| K -I -m sakurapool --help / index --help / remote --help | 0/0/0 | PASS |
| runtime multicategory/differential/runtime、localpartition/nested/partitioninventory/r1bridgemanifest/rustaudit八文件 | 0 | 113passed1skip34.60s |
| integration+两mode初次定向 | 1 | 5pass4fail96.12s，新test误用json_offset/loadpackage关键字；修实际API后9pass121.61s exit0 |
| R2production/p4package/p4transport/p4cli/p4tokenbootstrap+integration | 0 | 184passed1skip1039.72s |
| K -m pytest tests -q -p no:cacheprovider | 0 | **631passed3skipped0failed，pytest2080.39s，wall2081s**，634终态无筛除排序 |
| K -m ruff check .（最终） | 0 | PASS |
| git diff --check origin/dev...HEAD（实现后） | 0 | PASS |
| cargo fmt --manifest-path rust/Cargo.toml --all -- --check | 0 | PASS |
| cargo test --manifest-path rust/Cargo.toml --locked --all-targets | 0 | **58pass0fail** |
| cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings | 0 | PASS |
| cargo build --manifest-path rust/Cargo.toml --locked --release --bin sakurapool-worker | 0 | PASS，当前worktree重建 |
| K -m build --wheel --no-isolation --outdir .../index-integration-wheel | 0 | 新integrationwheel |
| uv pip install --python K --reinstall-package sakurapool <新wheel> | 0 | normaldeps noneditable，无no-deps |
| 仓库外 K -I .../wheel_verify.py --repository <integration> --wheel <新wheel> | 0 | **6pass148.18s**，33src对应wheel/current/sitepackages、runtime2/compilerv2 |
| uv pip install --python K --reinstall-package sakurapool -e 'D:/SakuraTool/SakuraPool[dev,remote]' | 0 | 恢复原主editable；实际-I module主src、runtime1 |

3skip：local_partition_builder.py337 WinError1314无法symlink；p4_package.py179无symlink权限；runtime_remediation.py787不能file symlink。无productiongate skip，不继承candidate外审统计。

wheel闭环对应：test_integration_index_builder_r2.py::test_local_remote_stage_equivalence_v2两个source参数（local singlepass/nested/P2v4/P3v2/valuequery/location/extentSHA且pairedremote）；test_r2_production.py::test_two_admin_modes_same_file_audit_and_stage_contract两个mode（R2P2v4/v2/packagevalid/recordid/audit/Rustexact/fresh）。另两节点multipartcompile和v1reject，共6；不是fullwheelsuite。

reviewer对实现SHA只读无阻断；实际full/wheel/Rust均绑定实现source，后续docs不漂移。只保一份full日志、一份wheel日志、可复验wheelhelper、简短plan/report；先设rawtext属性再stage，无额外hash清单。

## 限制与判词

production.py未改，merge未自然触及production_error；旧ok=true/result.production_error未统一映射、部分knownempty拒绝下游KeyError的非阻断限制保留，artifact/proof failclosed，不暗改源码扩范围。

网络binding/Range仍BLOCKED，旧canary cdn_status/HTTP数值UNKNOWN/2pending-bodybound2B保持；无探针退款。89.7GB FullStream保守inflight vs256MiB和4GiB组合disk不改，不减reserve/清root/换root，network两mode仍wholeTARspool。历史v1benchmark不更新，候选27.1GBdurable/95.9GBruntime仅LOWCONFIDENCE非承诺；本轮合成耗时非生产吞吐。

| 标签 | 结果 |
|---|---|
| P3_MULTI_CATEGORY_VALUE_MODEL / TAG_CATEGORIES_RELATION | PASS / PASS |
| SAME_DATASET_MULTI_PART / NESTED_JSON_ADAPTER | PASS / PASS |
| LOCAL_RUST_SINGLE_PASS_BUILDER / LOCAL_REMOTE_STAGE_EQUIVALENCE | PASS / PASS |
| R2_OFFLINE_TRANSPORT / R2_PACKAGE_RUNTIME_V2 / R2_FRESH_PROCESS | PASS / PASS / PASS |
| RUFF / RUST_TEST / RUST_CLIPPY / RUST_RELEASE | PASS / PASS / PASS / PASS |
| FRESH_WHEEL_LOCAL / FRESH_WHEEL_R2_OFFLINE | PASS / PASS |
| LOCAL_PREDOWNLOADED_PARTITION_BUILDER | READY，仅管理员已有TAR路线 |
| R2_NETWORK_DOWNLOAD_THEN_SCAN / R2_NETWORK_REMOTE_STREAM_SCAN | BLOCKED / BLOCKED |
| FULL_INDEX_BUILD_READY_FOR_REVIEW | YES，仅local路线待外审/另行真实构建批准 |
| FULL_INDEX_BUILD_STARTED / FULL_INDEX_OBJECTS_SCANNED | NO / 0 |
| REAL_MODELSCOPE_REQUESTS | 0 |
| DEV_UPDATED / MAIN_UPDATED / P5_STARTED | NO / NO / NO |

INDEX_BUILDER_R2_INTEGRATION=WAITING_REVIEW；仅pushintegration，停止，不自动merge dev/main、不启动22792TAR。
