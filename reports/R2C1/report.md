# P4-R2C1 完整提交报告 — WAITING_REVIEW

## 1. 结论与授权范围

资源/错误语义实现及最终产品源码的离线认证完成。原唯一真实批次在CDN403停止；用户随后直接要求“请求头是做什么的，使用请求头尝试一下”，root核验该事件后授权一次独立UA/Accept对照。追加实验严格1B Range成功，但未验证If-Match，不能宣称生产网络builder READY或P4完成。两个批次分别保留结果和不可删除sentinel，现停止所有真实操作。

唯一写入worktree=`D:/SakuraTool/SakuraPool-dev-finalize`，分支dev。没有修改旧detached dirty worktree、独立索引机器或历史reports/R2；没有main合并/强推/amend/reset/未知文件清理。

## 2. Git与源码绑定

|角色|完整SHA|完整tree|
|---|---|---|
|BASE|76ccce256ae08c5cd6a6569f4a78c874f84076e3|019a0c1ddca6230f1a4fb89eff632dd52187b9e5|
|START：错误语义|e0c82a7ca4c48ffd6ee90798e1e00e99ec90de0a|134b20bce4eed5e3974dd846c5c608a705f39c52|
|异常链修复|865c39d8038032279c32b06c3a0ff14aa8e1badb|80e706aff87c0022bad3740c98e8bc3b1b96d799|
|原资源冻结|6a6010bfdb1daaff271c5ef3d4e046d5fd8616d4|9bb5474631bef4d8ad86734e5de8f2a93777f0fa|
|END：用户追加公共header|f7c4658dac7ff6e5db977e8b2d53db504d5baaaf|3d59f19156ab90b1e18565d922f6e801c338c204|

全部四个中间实现提交均普通push至origin/dev并核远端SHA。固定main=`edc72fbaaddc4dc8f9865ddfa576b737c7b15f5a`。证据SUBMISSION的实际完整SHA/tree、远端核验在交付正文给出：提交无法在自己的tree内部自指自己的SHA。本证据提交不修改src/rust/tests/pyproject；END至SUBMISSION这些路径零diff须实际验证。START不是BASE。

## 3. 契约和实现闭环

P2 durable格式4/P3 runtime格式2、TagKey(namespace,value)、tag_categories/nested_json_v1/multipart、ObjectId/RecordId、local Rust单遍、worker NDJSON v1保持。依赖及Python>=3.10未改；无格式升级/publication扩展。

- `ok=true/result.production_error`仍是业务失败。可信完整accounting先结算一次，再抛安全错误；partial/malformed/dead-worker保留pending。合法wrong-condition412空body作为negative proof，不误伤。raw错误cause/context/last_result泄漏一起关闭。
- Range按requested<=8MiB+fixed buffers准入，分块写/hash，不因whole object size乘数预留。1B模型memory33,554,434B/transferdisk16,385B。
- Download真正whole-body spool，body/scan.json/metadata.bin一并保留至本job预冻结intent及独立验证的完整P2 COMMIT一致。adapter、provider/repo/dataset scope、path、size和所有identity均绑定；任意同ObjectId的另一个P2不能自证释放。未知/失败路径不会按名称删除。
- Remote HTTP直接流入现有Rust TAR scanner noncollect observer，不whole-TAR/image spool、不全members Vec/JSON。local collecting wrapper保留，不另造parser。Python读有界sidecar并核hash/count/extent/metadata结构；image丢弃后不能独立重算imageSHA，明确依赖同次Rust scanner。Download另验ownedspool。
- owned snapshot在nofollow/reparse/nlink检查后登记；nestedfinally避免settlement失败跳过cleanup/登记，保primaryerror。stat→unlink依赖job独占，不是对恶意外部替换的原子证明；partial unlink可能长期留quota。不宣称自动reclaim/网络pending退款。

## 4. 资源上界与实际限制

磁盘cap4,294,967,296B/inflight268,435,456B保持；生产scan/P2 job memory admission128MiB。Range<=8MiB；members<=100,000，path4096B，record line32KiB，实际escaped records合计32MiB+footer4096B；metadata单member1MiB/合计32MiB。数量界和实际序列化界共同生效，不承诺100k任意长路径可接受。JSON结构先检查depth32/tokens32768再decode。

生产stage main128MiB/reserve129MiB；两P2 spool各60MiB；跨所有对象fragments共享192MiB；audit64MiB。row<=512KiB，nominal batch256KiB；允许<=512KiB的oversized singleton，不能称每批严格256KiB。Parquet production rowgroups每表<=512，无dictionary/compression/statistics。历史1152MiB durable/build allowance不抬高。

仅fresh disposable private生产stage/spools核验journal_mode=OFF，固定page4096、boundedcache/nommap，错误终止、不恢复/复用失败DB；local默认DELETE不变。没有猜SQLite VFS sector/journalheader上界，不用journal_size_limit假装hardcap。page cap含索引；extentindex空表建好逐insert维护。固定SQL计划及bounded readonly reader/cache避免ORDER BY/lookup TEMP B-TREE；tempMEMORY本身不是上界，依据是具体SQL。

Stage DB显式close/nofollow安全fsync后hash，最后fsync marker，hash不等durability。P2 fragment/reference验证、spool关闭、audit fsync/hash在COMMIT前。多对象COMMIT不是原子事务，中断可能partial marker而整inventory拒绝；全部COMMIT后settle失败可留下有效data+pending，不能说每种failure都无marker。

阶段max而非历史sum：A=transfer，B=A+129MiBstage，C=128MiBstage+1152MiBdurable+仍live全部Download transfer；Remote sidecars在stage结束可释放。两spool120+fragments192+audit64=376MiB及rounding位于durable allowance内。

**实际ledger仍physical+future reserve保守双计，没有materialization handoff。** 大Download可能比结构模型更早拒绝；不人为降reserve、抬cap、删pending绕过。allocationcluster前提不是SQLite VFSsector证明。

**128MiB是有界buffer/decoded对象/rowbatch/cache/footer及native allocator headroom的admission模型，不是OS强制全局RSS数学定理。** worker结束后才Pythonstage/P2，阶段不重叠；serialized大小不直接等于decoded RAM。采样与定向测试支持该模型，但不能由workerRSS证明Python/native全pipeline硬峰。

## 5. 合成专项证据与性能口径

均为小型合成数据，不是生产吞吐/benchmark，5ms采样只是观察峰非瞬时数学最大值：

|TAR B|模式|观察jobdisk B|观察rootdisk B|workerRSS B|
|---|---|---|---|---|
|67,112,960|Remote|32,768|2,134,016|23,179,264|
|134,225,920|Remote|32,768|2,134,016|23,359,488|
|67,112,960|Download|67,145,728|69,246,976|23,150,592|
|134,225,920|Download|134,258,688|136,359,936|23,068,672|

真实写入非sparse/truncate。root含fixture/ledger基线，两个body样本仅两members证明body解耦，不代表密集metadata峰。dense metadata28,832,400B：Python/native baseline82,276,352B、sample absolute105,373,696B、delta23,097,344B，同5ms采样/reserve134,217,728B，不是普适RSS硬界。

三路local/Download/trueRemote对stage rows、whole/memberSHA、extent/size、dataset/storage/object/record identity、source/provenance、text/tag/category/nestedmetadata和P2表比较；multipart保重复posts，P2→runtimev2query/offlinepackagefetch验证。100k noncollect普通路径及serialized/metadata拒绝边界有Rust测试；消费者也有bounded reader/共享fragment cap。详resource-design.md及测试代码。

## 6. 环境、命令、测试与首失败记录

Keeper=`D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env`，Python3.13.15；实际导入当前`D:/SakuraTool/SakuraPool-dev-finalize/src/sakurapool/__init__.py`，PYTHONPATH unset。Rust/cargo1.98.1 WindowsGNU；CARGO_TARGET_DIR=`D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target`；显式worker同target/release/sakurapool-worker.exe。没有第二长期env/target，两轮repo外wheel专用短期env完成后均安全回收。

```bash
cd D:/SakuraTool/SakuraPool-dev-finalize
unset PYTHONPATH
export CARGO_TARGET_DIR=D:/SakuraTool/SakuraPool-Fix3-20260930a/rust-target
export SAKURAPOOL_RUST_WORKER=$CARGO_TARGET_DIR/release/sakurapool-worker.exe
PY=D:/SakuraTool/SakuraPool-Fix3-20260930a/python-env/Scripts/python.exe
"$PY" -m ruff check .
cargo fmt --manifest-path rust/Cargo.toml -- --check
cargo test --manifest-path rust/Cargo.toml
cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
cargo build --release --manifest-path rust/Cargo.toml
"$PY" -m pytest -q -p no:cacheprovider -ra
```

最终END f7c4658 tree：Ruff/fmt/Clippy/release退出0；Rust61pass。header/auth隔离/strictRange/location/diagnostic等Python定向23pass259.56s/退出0。**最终完整Python671pass/3skip/0fail，2243.72s，wall2245s，退出0**，日志final-python.log。

三skip均Windows无symlink权限：test_local_partition_builder.py:337 Win1314；test_p4_package.py:179；test_runtime_remediation.py:787。不称通过。

原6a6010b完整Python671pass3skip3224.52s/wall3225s/0（full-python.log），原新installedwheel14pass245.19s/wall248s/0（fresh-wheel.log），只绑定原freeze。最终f7新build/安装repo外非editablewheel，-I/noPYTHONPATH/显式新releaseworker，35installedPython/sourcefiles逐byte一致；header两hop/auth隔离与两admin mode P2/runtime chain **3pass148.56s/wall150s/0**（header-wheel.log）。Python源码未改，但Rustworker已变，不能拿旧14pass冒称最终新worker覆盖。

Wheel命令：keeper `-m build --wheel --outdir <本轮独占temp>/dist`，`-m venv <temp>/env`，该env `-m pip install <新wheel> pytest==8.3.4 requests==2.32.5`，repo外该env `-I reports/R2C1/wheel_verify.py --repository <repo> --wheel <wheel> --headers-only`；原14项未用headers-only。35file检验不含Rust源码，Rust通过显式刚构建的worker来源绑定。

首失败没有删除/改写成连续PASS：错误语义首提交41pass594.24s、exception closure5pass46.32s；早期fixture字段stage.images/错误URL/API、sampling基线、跨卷hardlink、capacity未算ledgeroverhead修正。Windows未耗尽cursor造成真实teardown产品缺口，明确cursor.close后修复，不忽略teardown。联合prefreeze65pass1fail1058.40s，旧2GiB拒绝阈值新模型可容；改实际model boundary仍确保beforemetadata拒绝，followup18pass353.87s，最终full通过。初次原wheel env缺pytest ModuleNotFoundError退出1、无tests运行；补依赖后14pass。初次普通commit失败exit128后正常提交，不编造未记录原因。证据暂存首个diff-check退出2：Windows raw日志CRLF被当trailing whitespace；以仅日志属性 -text/whitespace=cr-at-eol明确保留原始字节后重验，不改日志或跳过实现lint。plan记录其他已知中间统计。

## 7. 真实批次A：原授权单batch，严格失败停止

目标ModelScope repo=`leafmoone/game_cg_5M`、hub218032、object=`pre/gamecg-v1-pre-p02-003.tar`、size1,401,159,680B、revision candidate=`77948d890f654e6151a5ff7597b63a8337d8910e`。candidate不是immutable version binding。

脚本real_probe.py，root前置审核后`--root-prerequisites-passed`仅一次exit3。control<=3logical/每logical现有GuardedTransport<=3attempt，安全redirect/有界retry已披露，不能称整个batch完全no-retry；无SDK/outerretry。计划control<=9attempt/9,437,193B含overflow，Rust<=3logical/6attempt/6Breserve，总<=15attempt/9,437,199B，Range1memory33,554,434B/disk16,385B，由ledger实际cap再拒。

实际control3logical/3attempt/99,999B；Rust1logical/2attempt。origin302/CDN403，Content-Length numeric null，Content-Length/CR/ETag/encoding presence flags false，均仅诊断观测；CDN status拒绝发生在framing验证前，不证明服务端没有这些头。observedbody0B但accounting不完整。不能当socketactual零消费、不能猜403是token权限或过期。strictRangeBLOCKED即停，正负If-Match NOT_RUN，bindingBLOCKED，无后续logicgate。

|ledger|前|后|变化|
|---|---:|---:|---:|
|attempts|21|26|5|
|knownsettledbodyB|46,987|146,986|99,999|
|metadataB|46,986|146,985|99,999|
|body incl pendingB|46,989|146,990|100,001含unknown2|
|pendingleases|2|4|2|
|unknownbodyboundB|2|4|2|
|physicalrootB|2,749,554,688|2,749,554,688|0|
|inflightB|0|0|0|

旧2leases/2B+新2leases/2B均保留；real-round.started不可删/重启。

## 8. 真实批次B：用户追加UA/Accept对照成功，但非因果证明

SakuraMoon公开源码dev81f25fe23ed68c36694288475226eb1c53f48c03/src/sakuramoon/data/modelscope.py由swe源码核验，非reviewer独立来源核验。_headers设置`User-Agent: SakuraMoon/1`、`Accept: application/json, application/octet-stream`，_open每hop应用，认证只给ModelScope原站。仅追加这两静态公共header，无任意header配置、无代理/redirect/cookie/Range/IfMatch/budget更改。

新header_probe.py由root/reviewer审查后`--root-header-prerequisites-passed`一次，独立header-round.started/结果，exit0。control0logical/0attempt/0B，只复用批次A已独立核验的公开target/path/size/revisioncandidate，不fresh revalidation、不复用旧signedURL/ETag。产品仍新取得origin→CDN地址。允许1logical/<=2attempt/<=2Breserve、memory33,554,434B/disk16,385B。无If-Match或proof registration/fullstream/scan/outerretry。

实际1logical/2attempt，origin302/CDN206，body1B/Content-Length1/Content-Range精确0-0/total1,401,159,680、strongETag存在、accounting完整。ETag实际值不保存。

Ledger attempts26→28；knownsettledbody146,986→146,987、totalinclpending146,990→146,991；metadata146,985不变；pending4leases/unknown4B不变，physical2,749,554,688/inflight0不变。无新增unknown，无旧pending退款。

**当前这组header的1B Range路径成功，不证明header是前次403的唯一原因**（签名/时间等改变）。仍不完全等同SakuraMoon服务：Rust不读env proxy、更严格跳转/Range/资源约束。正负If-Match仍NOT_RUN、VERSION_BINDING仍BLOCKED，不升级两networkbuilder。

两次独立授权合计：control **3logical/3attempt/99,999B**；Rust **2logical/4attempt/观察1B**且一批accounting不完整；实际HTTP attempts **7**。attempts **21→28**，known body **46,987→146,987**（新增100,000B=metadata99,999B+Range1B），metadata新增99,999B；bodyinclpending **46,989→146,991**（新增100,002B含unknownbound新增2B）。新增unknownbound2B不是消费。此合计由两脱敏账本快照算术核验。

两批仅typed数字/flags与固定身份：signed_url_persisted=NO、credentials_persisted=NO；不持久rawURL/Location/ETag/CDN/token/cookie/providerbody/exceptionchain。TOKEN原站Bearer+同源m_session_id，CDN不带原站认证。原结果不覆盖，旧pending/sentinel不删。停止全部真实操作，不因结果再改代理或加参数。

## 9. 最终标签及停止边界

```text
P4_R2C1=WAITING_REVIEW
PRODUCTION_ERROR_SEMANTICS=PASS
RANGE_RESOURCE_MODEL=PASS
DOWNLOAD_RESOURCE_MODEL=PASS (ledger保守双计限制仍在)
REMOTE_STREAM_RESOURCE_MODEL=PASS
REMOTE_STREAM_NO_WHOLE_TAR_SPOOL=PASS
BOUNDED_MEMORY=PASS (buffer/对象+admission模型，不是OS全局RSS硬限)
BOUNDED_DISK=PASS (实现cap/owned寿命，采样不是数学peak)
LOCAL_BUILDER=READY
DOWNLOAD_BUILDER=BLOCKED
REMOTE_STREAM_BUILDER=BLOCKED
REAL_RANGE_CAPABILITY=PASS (仅用户追加header1B对照；原批次BLOCKED)
REAL_IF_MATCH_POSITIVE=NOT_RUN
REAL_IF_MATCH_NEGATIVE=NOT_RUN
VERSION_BINDING=BLOCKED
P4_COMPLETE=NO
INDEX_FORMAT_CHANGED=NO
P2_FORMAT=4
P3_RUNTIME_FORMAT=2
MAIN_MERGED=NO
P5_STARTED=NO
```

资源闭环完成≠P4完成；Range成功≠versionbinding。无全TAR/22,792对象构建、1M/5M benchmark、R2C2 publication/package-v2/P5、生产数据服务或大规模任务。验证为小型合成数据，真实端点仅上述有限requests。完整证据提交正常push dev核main后停止WAITING_REVIEW。
