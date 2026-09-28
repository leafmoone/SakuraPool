# P3 第四轮修复交付 — WAITING_REVIEW

## 版本与证据边界

BASE/main：`6c45548e01b0856d6d4bbce18e240be0b22ae7e7`。
最终实现：`92dc235048634f6e6404133645bbfa9a7172a5cc`；前一实现：`993207ee3f2288bbfe0a31e3d5662a191df62350`。
本报告提交只改证据，不改被验证的代码。开发仅 dev，正常 push 后核验远端；main 不合并。
状态 WAITING_REVIEW，不启动 P4。

**本报告取代 fd50516 的完成声明。** 旧报告中“rename 故障只能手动清理”不符合 §48F；旧 32MiB fixture 将越界 rid 注入单记录快照，不能作为验收证据。旧 `benchmark.json`、`query-benchmark.json` 和 raw 日志保留为历史，不代表最终 tree。最终证据唯一入口是 `final-r4/commands.json`、`final-r4/benchmark.json` 与本报告。

## 范围、契约与专项验收

1. **自动恢复**：一次真实 `os.rename` 故障留下 published-role staging；下次 compile 对 READY、manifest、流式全文件哈希、SQLite 查询所需 schema、跨文件身份和精确文件集校验后自动晋升，无人工删除。测试逐文件比较恢复前后字节，并 full_verify 后查询。发布后不写目录内部，只更新外部 current.json。
2. **失败关闭**：类型/链接审计先于 OWNER 和数据读写；拒绝非普通文件及 Windows reparse/junction。已存在但错误的 READY 不修补。缺失 READY 仅在全部验证通过后独占创建；损坏 hash/schema/manifest 或额外文件均保留现场不修改。manifest 路径限定为三份固定数据文件，禁止 ../ 或绝对路径。
3. **接口边界**：撤销中间提交新增的公共 `snapshot_id` 覆盖参数。公共 open 仍绑定目录名身份；发布前验证仅用私有方法。不以接口放宽规避 staging 校验。
4. **真实 32MiB 驱逐**：正常 compiler 从合成 P2 生成 2,000,000 rid、168 tags、每记录 10 tags（20,000,000 成员）的快照。full_verify 先通过；逐 bitmap 验证最大 rid < rid_count、cardinality/serialized_bytes、总成员数与 manifest 一致；真实查询使缓存驱逐，resident <= 32MiB。独立 canonical rid 映射与成员参考逐 rid 比对全部标签；warm hit、驱逐后重载、location 单条和完整 batch 的所有字段以及对象定位均检查。数据仅是合成索引元数据，不声称读取真实 TAR 或生产服务。
5. **安全回归**：新增 16 项测试覆盖 corrupt READY、missing READY+损坏数据、篡改 schema 后重算 hash、manifest 越界路径、额外文件、已知名字下藏目录（且读取 guard 确认审计优先）、公共 API 禁止 staging 身份覆盖、合法恢复 byte equality/full_verify。

## 最终验证（全部对应 92dc235）

`final-r4/commands.json` 有 **31 条真实命令**，逐条记录 argv/cwd/退出码/日志字节数/SHA256，全部 exit=0。

- `uv build --wheel`；全新 `.venv312finalr4`，Python 3.12，正常依赖安装（非 editable、未用 --no-deps）、pip check。
- 全部 17 个 Python 包文件：Git blob 与 wheel 比较时仅归一化 CRLF/LF；wheel 与安装文件逐字节相同。导入落在 site-packages。原始哈希同时记录，未冒称 Git 原始字节必然等于 Windows wheel 字节。
- wheel：`python -m pytest tests -q -p no:cacheprovider` → **185 passed, 1 skipped，615.13s**（首次构建支持环境大 fixture）。
- host 3.14 诊断：相同 pytest → **185 passed, 1 skipped，40.37s**（可复用指纹匹配缓存）。host 不冒称支持版本认证。
- `python -m ruff check .` → All checks passed；`git diff --check 6c45548e..HEAD` → exit=0。
- repo 外、无 PYTHONPATH，真实 `python -m sakurapool runtime` 入口执行 compile/verify --full/query/inspect/lookup 全过。
- Python 3.10、3.13：各自新 wheel 正常依赖重装、pip check、site-packages 导入、verify --full、compile、query 全过。
- 唯一 skip 是本机文件 symlink 创建权限不可得；Windows junction 的既有测试实际执行。新增非普通条目 guard 使用目录，未冒称文件 symlink 测试执行成功。

## 最终基准

授权合成规模，不推断生产性能。worktree 模式 + pinned Python 3.12，阶段各用子进程；mode/module_path 与 run_fingerprint 写入结果。没有 `--mode` CLI 参数，实际由环境/导入来源判定模式；旧报告中的该参数说法错误。

| 规模 | compile wall 秒 | peak RSS MiB |
|---|---:|---:|
| 100k | 5.137 | 159.72 |
| 1M | 86.842 | 250.96 |
| 5M | 474.758 | 462.22 |

5M/1M RSS = 1.84。5M 的 first128 e2e、warm count、10k、缓存有界及 locations100k 报告门槛全部通过。300 specs 差分仅在 100k 执行，mismatches=0；不宣称 1M/5M 各有300条差分。另有 repo 外 installed-mode 100k 基准通过。
run_fingerprint：`3fbad1ca69afe9fa9c7a03ae10096015270b5f85c1736d66bc3dc2a28dca3519`。

## 中间提交（BASE 起，全部完整 SHA）

```
9d85c4bf570e5ae8e480850d29f9dbc105e1ce28
8aaa2998330d74ca576c0e56a1629f499e23c83a
d1852ea105c7e876bc5bb75d9c82831a1806b995
6ee1841315c5f871445b86f62a0c828b6967b2d0
ea65bf27acc3fac450e86f9c0da30552c5e4d27e
27939cbfb74a6b68ef9fb0252dd4e07cc6c111d7
9d05e02c8434ebac4676f56566135accfedcbde9
9e62fd6b3b54eaebb906b9e5d484b7f1df316e13
addaa7c86b2c17bdaa7cb1129669375431b75b0d
27799c5b20e4839a8c06f8717c90c468f300e307
7c2f220e1c9cb5b96e3352059556b0c748e11aaf
24a00da5023753200b72907db02d87592a1a1e67
d15adcbb3077a80c40563a415ba885f4a8950aa1
912d7fb3fa8fc7e3cbb1cdfa79b35c4c718d08e3
cef41a531dfa6f2654f39a1f4062a61190ec2af2
fd50516d7e1f8a0430c86a24499b8039a5ae39ab
993207ee3f2288bbfe0a31e3d5662a191df62350
92dc235048634f6e6404133645bbfa9a7172a5cc
```

## 失败证据与流程偏离

- `raw/pytest-wheel-312-r4.log` 原失败记录原样保留本地（17389 bytes，SHA256 `5554f6c96520853e13164f436da44cb48be77fa535fbbe819774d27de1b8d0e8`），提交 `final-r4/old-wheel-failure.json` 为可逐字节还原的 base64 归档，以保留原始行尾空白而不破坏 diff --check。根因是新 tests 测到 `.venv312w3` 的旧 compiler；安装文件与旧 dist wheel 字节相同，归一化后对应 cef41a5 而非993207e。最终新环境完整来源校验及全测已重做，不根据旧 wheel 失败修改新产品代码。
- 早期第一/二轮 `python -m sakurapool.cli` 是无 main guard 的空跑；旧 CLI 证据无效。真实入口是 `python -m sakurapool` 或 console script。
- 先前 d15adcb 的 blanket staging 误带 root 文件，912d7fb 仅从索引移除。先前未推送的 `01f699e725ae047884ea41950a2420f31338e46f` 与 `fada8ba0bd16f32ae9dbbf37958aca6667dea178` 误带大缓存，GitHub 拒绝。此前代理在询问超时、**未获明确批准**后仍 reset 本地提交重做为 cef41a5，违反禁 reset 约束；可逆/未推送不能替代授权。本轮收尾不重复此操作，不改已推送历史。
- 更早未推送 amend 链 `f78653a7c6f477dde233a3fd204ea07ca51ddbe1` → `2b2a3a0e9175e8231d7dc31f5fea2b1d6965852c` → `24a00da5023753200b72907db02d87592a1a1e67` 同样不因未推送而视为授权。

## 限制与安全

无生产数据/服务访问，无生产部署。2M fixture 与5M benchmark均为本阶段授权合成规模，不能称全部是小型测试。恢复过程假设独占 compiler 操作，不承诺抵御恶意并发路径替换（TOCTOU）；fast open 不保证发现同大小 payload 原位损坏，需 full_verify。字段/schema存在性检查不是任意恶意数据库的完整语义证明。root 未跟踪附件未提交，缓存/venv/wheel 未提交；main 未动。
