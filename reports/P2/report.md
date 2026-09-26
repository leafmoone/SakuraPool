# P2 实现审计与交接（待审，不合并）

## 状态与范围

- BASE/main 核验：`52358d6fca728d2bba12814490e0974a6907b218`；接管时 dev 与 main 相同。
- 接管已有未提交 `indexer.py`、`test_indexer.py`、CLI 和包导出，未丢弃已有功能方向。
- 仅本地合成 TAR；未使用生产数据、网络数据、P3、下载或模型运行。
- 状态：**WAITING_REVIEW，有环境与需求追溯缺口，不宣称 P2 全部验收完成**。
- IMPLEMENTATION/SUBMISSION 的完整 SHA 由最终交接报告提供；本文件不能自包含自身提交 SHA。

## 真实根因

首测确实复现 `summary["objects"] == 0`，预期为 2。跟踪 `os.fsync` 调用发现旧
`indexer.py:253` 对 `marker.open("rb")` 得到的只读描述符 fsync，Windows CRT 返回
`[Errno 9] Bad file descriptor`。旧 broad catch 把发布错误吞成 `archive_error`，
对象计数发生在错误之后，因此 fragments/COMMIT 已存在却计数为 0。
修复不是改成期望 0：首测仍要求 2；持久化统一可写 `.partial` flush/fsync/rename，
COMMIT 最后原子发布，I/O 错误向上传播。补充真实进程退出与存储错误恢复回归。

此外修复旧实现 basename 跨目录配对、每对象而非每 shard 提交、无输出哈希校验、
无效 tags 列表元素、声明 hash 不校验、未提交版本号与包元数据不一致等问题。

## 需求—证据映射

| 要求 | 实现及验证 |
| --- | --- |
| registry adapter / canonical source | DatasetAdapter/AdapterRegistry；配置校验、字段映射、完整相对路径 pairing、source mismatch |
| 未压缩 TAR、真实 offset/size、no decode | `r:`；uint64；gzip/bzip2/xz 拒绝；PAX 长路径；真实 seek 回读；非法图像字节仍可索引 |
| 稳定身份 | canonical JSON 的 dataset/source/shard/member/extent 哈希；不同 shard 不碰撞；两个全新进程输出逐文件 bytes 相同 |
| 四种 PyArrow 表 | objects/samples/annotations/errors，空表保留 schema；P1 原有 DEFAULT_SCHEMA/查询/模型不变 |
| tag/hash 来源 | missing/invalid/empty/known；declared:json.sha256 与 computed:sha256 分开；非法声明记错误 |
| 每 shard fragment 事务 | 四 parquet + COMMIT；文件 fsync/同目录 rename；COMMIT 记录每文件 hash/bytes/rows 和输入 validator |
| resume/partial/corruption | stale partial 清理；未提交重建；已提交四文件哈希/schema/行数校验；有效但篡改的 Parquet 亦拒绝 |
| input 变化 | 全输入集 + size/mtime_ns/SHA256 + adapter 配置；同 size/mtime 内容变更、touch、增删、扫描中变化、hash 参数变化测试 |
| CLI | `index scan --config --dataset --input --output`；保留 positional；JSON summary；row errors=1、fatal=2；截断 TAR 返回 JSON |
| A–H fixtures | A flat、B nested、C missing/orphan、D invalid metadata、E ambiguous、F source mismatch、G duplicate、H unsafe |
| crash A–E | A partial fsync、B 首文件 rename、C 四文件落盘、D COMMIT rename、E 发布结束；异常注入和子进程 os._exit(73) 均测试 |
| fresh process/roundtrip/determinism | 独立 Python CLI 两次构建逐文件比较、第三次 resume；真实读取长路径成员 byte extent |
| 10k benchmark | tools/benchmark_indexer.py；header/json/parquet/total 秒、进程 peak RSS；原始 JSON 在 benchmark.log |

## 契约说明

完整 schema、身份公式、错误语义与目录协议见 README 的 Version 1 on-disk contract。
这是新的 P2 四表契约，不把 P1 的 id/value/label 伪称成已有 P2 四表。
旧未提交 per-object 布局不迁移，必须新建 output；summary skipped 改为 shard 数，
objects/errors 是含已恢复 shard 的总数。sample_id 当前与 object_id 一一对应。
这是 occurrence 身份，不是内容去重；SHA256 validator 会读取全部 TAR 字节但不 decode。

## 原始验证证据与复现

执行 `python tools/verify_p2.py`（脚本为子进程设置 `PYTHONPATH=src`、禁止 pip 网络）。
`evidence-manifest.json` 记录每条实际 argv、cwd、exit、原始日志 SHA256/bytes 和 wheel hash。
完整 pytest 含全部 P1 回归、P2 fixtures/recovery/boundary；专项日志额外单跑配对与恢复。
ruff 检查 src/tests/tools；build 为 `python -m build --no-isolation`，生成 sdist/wheel。
生成的 TAR 和 index 均在临时目录，验证结束清理，未提交二进制 fixture。

### 不掩盖的环境失败

当前仅找到 Python 3.14.5，实际 Arrow 25.0.1 / pytest 9.0.3 / ruff 0.16.1；
pyproject 保留 P1 pins：Arrow 18.1.0 / pytest 8.3.4 / ruff 0.9.2。
`wheel-dependency-check.log` 原文：

```text
sakurapool 0.1.0 has requirement pyarrow==18.1.0, but you have pyarrow 25.0.1.
```

该命令 **exit 1**。fresh venv 中 wheel 以 `--no-index --no-deps` 安装，只离线复制
当前 Arrow 包，不开放 global site-packages；从临时 cwd、`-I` 运行已安装 wheel CLI。
因此 wheel smoke 成功只证明安装包内容和 CLI 行为，不证明 pinned dependency 安装成功。
验证脚本 failures 字段统计其主要测试/build/smoke 命令；依赖诊断单独记录，不能据此
把 `failures: []` 解读为 pinned 环境已通过。需要在具备兼容 Python/离线 pinned wheel
的环境重跑；本次未下载或悄悄更新依赖。

## 风险 / 尚未满足的验收项

1. 没有当前任务摘要之外的原始 P2 需求文档/最初 A–H 与 crash A–E 定义，故这里只能
   提供明确映射，不能声称和不可见原文逐项等价。需要协调方/审阅者核对。
2. pinned Arrow 18.1.0 clean install + 全套验证未完成，上述依赖检查明确失败。
3. Windows 上仅验证进程崩溃恢复，不承诺断电时目录 rename 持久性；POSIX dir fsync
   分支未在本机执行。并发写入不支持，调用方必须保证单 writer、输入静止。
4. 内存与最大 shard 成正比；10k 是合成性能观测，不是生产吞吐/内存上限承诺。
5. 未实现 dataset alias 自动规范化、非 WebDataset 自定义配对 DSL、跨 shard JSON 配对、
   内容去重或外部数据集专用适配器；如果原始 P2 要求其中某项，则仍是明确缺口。
6. COMMIT hashes 防意外损坏，不提供敌手同时重写 COMMIT/输出的认证防篡改。

已按 git-commit-flow 做身份/分支/diff/test 预检及明确路径 staging；不 amend、不改 main、
不 merge、不 push。提交后交接 WAITING_REVIEW。
