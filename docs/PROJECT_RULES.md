# SakuraPool 项目工作流程与边界

本文件是项目唯一的当前工作流程与边界说明。阶段执行单决定本次授权；本文记录产品方向，不代表未实现能力已经存在，也不替代阶段授权。

## 1. 产品路径

SakuraPool 是大型 TAR/WebDataset 的查询、定位和选择性提取工具，不是全量爬虫、去重平台或训练系统。

- 管理员链路：远端 TAR 的一次性索引构建 → durable 索引 → runtime 编译 → 校验 → 发布到数据仓库固定文件夹。
- 用户链路：安装／校验已发布索引 → 查询 tag/source/dataset → 定位图片与 JSON → Range 读取目标字节 → 输出本地小数据集。

远端扫描、索引发布和 Range 提取是后续阶段能力，不是当前 P3 已实现能力。普通用户不需要完整本地 TAR；P2 LocalTarScanner 保留为测试、本地数据输入和校验能力，不是线上用户的前置条件。

索引扫描可能读取完整远端对象，必须单独计量网络字节与临时空间；不能把“不落盘”称为“没有下载流量”。

## 2. 索引现状与构建时机

目前没有可直接导入的现成索引，将来由管理员扫描一次 TAR 建立。不再把搜索 hfutils/CheeseChaser 旧索引设为前置任务。

P4 runtime 以合法已发布索引作为输入契约开展开发；经授权的小仓库可先生成测试索引完成联调。“以已有索引为开发假设”不等于远端现在已有索引。

正式大仓库索引必须在所有开发及相应验收完成后另行授权构建，不得自动扫描、重建或发布。

## 3. 后续 P4 联调对象

指定 ModelScope dataset repo：`leafmoone/game_cg_5M`。登记该目标不授权本次访问、全量下载或写入，也不得从名称推断实际文件数量、大小、source 或标签词表。

P4 开始时按另行下发的执行单取得实际文件清单、对象版本与 metadata 结构，建立包含 TAR 数、样本数、网络字节和临时空间上限的 canary 清单。没有明确授权，不能将整个仓库当作“小 fixture”全量扫描。

## 4. 索引发布边界

预留可配置 `index_prefix`，建议默认 `sakurapool/`；这是拟定路径，不代表已创建。

- 发布物区分 durable 索引与供用户安装的 runtime snapshot；用户无需拉取全部构建期数据。
- 发布目录须有 manifest、格式版本、文件校验信息和明确的数据覆盖范围。
- 未全部扫描时只能标记 partial/canary，不得称为覆盖全库。
- 对象数据 revision 与索引发布 revision 分开记录；上传索引可能产生新的仓库提交。
- 索引 offset 必须绑定建索引时同一份 TAR 内容，不以浮动分支名代替版本验证。
- 具体目录及 manifest 字段由 P4 执行单审定；本轮不实现上传。
- 不上传临时 signed URL、token、完整凭证或本地绝对路径作为公共索引身份。

## 5. 对象与存储

`storage_id` 是 Storage Profile ID；`object_id` 是确定版本对象身份；`repo_type` 与 `archive_format` 分离。

当前 Local strong-SHA 输出与远端仓库位置须显式绑定并核验，不从目录猜 source，不静默改 backend。不推翻已验收 P2/P3；必要适配须明确提出、版本化并审查，保持 P2 durable v4 / P3 runtime v1 的已验收契约。

远端 Range 正常路径不得每次全 TAR 下载重算 SHA；普通下载也不得在 Range 失效时静默退回整 TAR 下载。

## 6. 组件边界

| 组件 | 职责 |
|---|---|
| TAR | 图片和原始 JSON |
| Parquet | 持久、可重建索引 |
| Roaring | 在线集合检索 |
| SQLite | 目录、身份、bitmap 容器和构建期暂存 |
| NumPy mmap | 位置数组 |

核心不引入 DuckDB，不为未来假想需求增加 hfutils/CheeseChaser 依赖或通用 importer 框架。去重仅作为未来下载后本地子集的独立后处理，本轮不实现。

## 7. 审查与 Git

- 开发、修复及规则修改都在 `dev`；正常 push 后核验 `origin/dev` SHA。
- `main` 只接受外部明确批准的固定提交，使用 `--ff-only` 合并；复验通过后正常 push 并核验远端 SHA。
- 每阶段交付完整、连续、可复制的报告，列明实际提交、验证证据、限制及远端 SHA；内部自审不能替代外部批准。
- 下一阶段先审上一阶段报告／commit，再确认 main 合并回执，之后取得下一阶段授权才能开始。
- 禁止 amend/reset/rebase/force push、自动 `git add -A`、删改他人未提交文件；显式暂存授权路径，不用重写历史抹去历史违规。
- 人工询问超时不等于授权。未经审查的规则提交也不能自动合入 main。

## 8. 验证证据

明确 source、wheel、installed package 对应 SHA，不用旧 wheel 测新 tests。新 workload 必须重新绑定 generator/options/code/environment 指纹，不能将旧规模输出拼成新证据。

使用真实 CLI 入口，记录命令、退出码、跳过项、日志字节数和哈希。源码未改不反复重跑 5M；代码或 workload 改动后按影响范围重测。保留历史证据，但明确唯一当前入口。

当前 P3 性能入口：[`reports/P3/bench-repair/benchmark.json`](../reports/P3/bench-repair/benchmark.json)。完整报告见 [`reports/P3/report.md`](../reports/P3/report.md)，installed 验证另见 [`benchmark-installed.json`](../reports/P3/bench-repair/benchmark-installed.json)。这些是合成 workload 证据，不是网络性能、真实全库或 21M 性能保证；旧 benchmark 不是当前验收入口。

## 9. 简洁与优化原则

正确性、数据完整性、资源泄漏及违背授权的行为必须修复。可测的重复 I/O、反序列化、排序是候选优化，需要前后证据。美观、拆文件、额外抽象或新框架不是独立合并门禁。

不能为减少代码而省略必要校验，也不反复修改已稳定的身份与格式。

以下仅登记为候选，本轮不实施：

- 编译阶段 Parquet 列投影，避免读取无用 tags/text。
- 删除同一路径中的重复排序／字典查询。
- 根据 profiling 决定是否缓存解码后 bitmap；不能共享可变 bitmap 污染结果。
- P4/P5 统一 ObjectRef／选择结果接口，不提前大改 P3。
- 管理员远端扫描只计算必要校验，避免复制当前本地多次全 TAR hash 行为。
