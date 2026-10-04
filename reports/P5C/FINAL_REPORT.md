# P5-C 集中优化收尾报告

## 范围与身份

状态为 WAITING_REVIEW；不是用户验收、main 合并批准或 P5-D 授权。本轮优化起点 `87306f4d7e5057f972f2cdda647dc86e41576c88`；实现提交 `1de5000219590379cbf5ffca193a0cbb114f9fc4`，tree `6d9e72ef6edd0a71bd4f7d3b67fd5301be59d7a3`。这是本轮唯一实现提交；报告随后单独提交，其 SHA/远端核验在交付正文给出。

原 sole 对冻结实现 tree `d4cfef2b9b9cc5c20e8bfe76449d9fe1c5cc28d0` 有限实质审 PASS；最小 stress 调整后 tree `08b9d0269e954b5c9f49f32a04d1d6624eb7b318` 用于性能、stress、初次常规回归。最终 tree 6d9 相对 08 仅修测试 fixture/断言，产品源码相同；sole 对此测试 delta 有限核 PASS。性能四行也获有限核 PASS，不等于生产性能认证。

## 实现与契约

- 实际选中 lane 后做 preflight，复用选中 PreparedFetch 的已验证投影；忙候选仍可能重复 prepare，不宣称每记录只 prepare 一次。
- warm 只用于调度/额度预测，publication digest、完整 ObjectKey、lane 与实际 generation/proof 状态共同限制资格；不替代 proof/reserve，不跨 lane 继承。轮换、close、版本变化和失败使预测变冷，UNKNOWN 不因缓存变成已知。
- 候选窗口 ≤2W（最大 8 薄 row），先验证整个窗口，不用 WHERE 跳过坏首项；精确 claim 在同事务核控制请求、候选及 attempt。忙 key 等待，不为了占满 4 lane 复制绑定；最老可选 key 优先。
- 保留 retained receipt 两次遍历：finish 故障可新增保留项，未合并成一次。completion 处理及故障遍历之后释放 descriptor；3-record 弱引用观测峰 ≤W+2，不外推到 8-record/RSS 硬界。
- 默认 workers=1、冻结 seq/selection/export、Range 边界、cap/fsync 与持久顺序不变：reserve→OUTPUT_RESERVED→IO/fsync/hash→PREPARED→rename→PUBLISHED→settle→SETTLED→DONE。
- 注册 stress marker 并标记唯一长例；没有改变 pytest 默认选择。仅明确使用 `-m "not stress"` 的命令排除它。

## 验证与真实失败记录

统一 keeper Python 3.13.15；actual310 为既有环境 Python 3.10.21。下列非特别注明均退出码 0。

1. 核心暖门快组：3 passed /79.16s。实际 Rust 同 lane 首对象 proof 后，用真实合成 ledger Reservation 占用剩 body 至仅够下一 5B Range+1B framing；第二记录成功，verify_conditions 仅 1 次/gen0。冷对照清真实 live proof、保 publication cache：下一记录 RESOURCE_BLOCKED、交付保持 1。测试专属占用 lease 在 finally 收尾，不退款 UNKNOWN。HTTPS publication 与 loopback byte-plane origin 仅 fixture 中中性翻译，不放宽产品完整 key；首未适配失败保留为 fixture 错误。
2. 亲和/descriptor/finish fault/UNKNOWN reconcile/pause-cancel 快组：11 passed /279.01s。actual Rust workers1/2/4 两对象各绑定一次（2 次 verify_conditions），不等于完整 HTTP trace；合成 descriptor 峰证据仅 3 records。
3. 唯一稳定后真实累计 stress：`pytest tests/test_p5c_c2.py::test_real_request_threshold_rotates_and_reprobes -q --tb=short`，1 passed /63.93s，tree 08。3 proof+252 transfer=255 时 `verified_object(...lengths=[1])` 保同 worker/gen0；第253次真实 transfer 达256；再 lengths=[1] 触轮换，gen1 重建 proof/Range，inflight0。这不是模拟 session credit 测试。
4. 常规回归：`pytest tests -q -m "not stress" --tb=short --durations=15 -rs`，tree 08，**退出码1：1 failed /1402 passed /7 skipped /1 deselected，2886.04s**。唯一失败 `test_settled_hold_is_not_counted_twice`：旧两 key fixture 的第一项延迟 completion 等第三 claim，而第三同 busy key 等第一 completion，形成测试自建等待环，10s timeout 被包装为 FETCH_UNCONFIRMED。不能称常规回归全通过；后续 Ruff 被 && 短路。
5. tests-only 修复：三个独立 key，保持真实 ledger/output cap15、workers2、延迟 completion 等第三 CLAIMED，新增 settled 已触断言；保 delivered3/saved15（不是显式 state COMPLETED 断言）。该 node+finish fault+actual affinity1/2/4+pause/cancel：7 passed /245.61s；全 `ruff check .`、diff/cachedcheck PASS，绑定最终 tree 6d9。按授权不重跑48分钟常规回归。
6. actual310 首误用前轮 noneditable wheel：3 failed /4 passed /63.87s，退出码1（旧 API 缺 prepared/predict_warm），不计当前源码兼容失败/通过。显式 export PYTHONPATH 当前 src、打印3.10.21及 runner 来源后，同受影响专项7 passed /133.54s。
7. `python -m build --wheel --no-isolation --outdir <owned-temp>` 成功。同既有310环境 `pip install --no-deps --force-reinstall <fresh-wheel>`；仓外 unset PYTHONPATH、`python -I` 核 runner/production 均 site-packages、direct_url 为 wheel 非 editable；`python -I -m sakurapool --help` 成功；一个实际 Rust 暖 ledger 代表任务：1 passed /1 deselected /42.74s。非整套 wheel 认证。

常规回归唯一排除：`tests/test_p5c_c2.py::test_real_request_threshold_rotates_and_reprobes`；它已单独执行上述一次。7 skip 是 Windows symlink 权限、literal question mark 路径限制及 native getlimit-absent 专属场景。Rust 源码未变，复用既有 release worker，不重复构建/全 Rust 测试。

## 同条件 before/after 小中图性能

before 固定 archive 源码87306（tree `2bdc09f8fec3828fe2394293b82fdb67f6602775`），after 产品 tree08，使用同一既有 Rust worker、同 instrument、workers4、8 records/2 objects、metadata0。每次新建同类独立物理计量域，计时前均8文件/4,231,171B，不沿用顺序残留；域扫描在计时外。caps相同，无生产删除/ledger迁移/隐藏retry。计时 run_task→transport.close/settle，不含 fixture setup/export；所有样本8实际文件长度/SHA、seq0..7、inflight0通过。

最终固定顺序：small before→after，再 medium before→after，每负载1 pair，不挑最好、不宣称统计稳定性/稳定2×/生产改善。

| 指标 | small before→after | medium before→after |
|---|---:|---:|
| 单图/总实际图字节 | 1,024 /8,192 | 262,144 /2,097,152 |
| wall秒 | 9.2919681→4.5944238 | 9.4540170→4.7791840 |
| wall下降（单pair） | 50.55489052% | 49.44811290% |
| object×lane/gen绑定 | 8→2 | 8→2 |
| protocol observe/match/wrong 1B probe，各次数 | 8→2 | 8→2 |
| image Range 次数 | 8→8 | 8→8 |
| provider lookup adapter 次数 | 8→2 | 8→2 |
| spawn /origin accept /CDN accept，各 | 4→2 | 4→2 |
| read_pair 次数/累计秒 | 348/6.93010→180/3.26614 | 348/7.03280→180/3.38374 |
| disk_usage 次数/累计秒 | 292/.51832→148/.22080 | 292/.53327→148/.23144 |
| write_slot 次数/累计秒 | 324/1.46169→156/.64333 | 324/1.50854→156/.69415 |
| Python fsync 次数/累计秒 | 656/.37991→320/.19043 | 656/.39487→320/.20486 |
| reserve 累计秒（次数142→70） | 3.75707→1.63638 | 3.82038→1.72035 |
| settle 累计秒（次数142→70） | 3.80345→1.75219 | 3.87762→1.80042 |
| coordinator calls.get 累计秒 | .038027→.067536（上升） | .036368→.060418（上升） |
| lane reply.get 累计秒 | 34.83666→8.10587 | 35.37608→8.39399 |
| bridge调用间隔最大秒 | 1.39342→.78902 | 1.45796→.81176 |

此前已经执行的 small pair 8.8724828→4.3137294s 保留；它的 queue 累计未分实例，因此不冒 coordinator 等待。更早8.57736s仅 harness smoke；旧共享域191s不是本轮before，不混比较。无新增采样要求，未调整 idle timeout。

原方法轻量包装，不增加逐操作 status/scan/日志/hash。安全原始四 SAMPLE 行为 tool job `bash_c2f13b01`；此前 pair 为 `bash_8f091d01`。复现临时脚本 `C:/Users/PC/AppData/Local/Temp/sakurapool-p5c-bench-4241/bench_closeout.py`，未另写日志副本。脚本同步 request hook 的 payload 层级不适用于 production，但这四行仅出现正确 raw send/read 分类，没有 None condition/length 类别；不无条件外推其分类正确性。

## 限制与安全

计数/累计 spans 为无锁多线程观测，不保证原子/不丢更新；nested spans 不可相加当 wall 或直接归因 fsync。Python 全局 fsync 覆盖多个 Python 子系统，不含 Rust；queue get 包含等待，reply 是多 lane 累计而非 wall；bridge间隔不是纯lanequeue。server close_request 为 socket cleanup，不证明TCP FIN/idle timeout；accounting 每 protocol request attempts=2来自Rust response，不是独立服务器HTTP hop计数。metadata0，lookup adapter 不是远端metadataHTTP；这是真实Rust两跳字节/ledger/task热点，非完整provider E2E。同域初态不证明OS缓存相同。RSS/handles/CPU、独立HTTP hop计数和8-record descriptor峰未测，不冒称生产性能或内存硬界。

本轮仅小型合成数据与loopback，不访问生产publication/task/pending、数据服务或index机；未重跑生产canary，未改main/强推/删远端分支/amend，未启动P5-D。最终dev正常push与远端完整SHA核验列交付正文；阶段停止送审。
