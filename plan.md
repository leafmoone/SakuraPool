# P6-A：用户 Workspace、显式资源策略与集中容量配置

目标：普通机器在明确用户workspace运行现有冻结任务；保持持久reserve/consume/settle、UNKNOWN、固定对象内容绑定与原子交付。当前执行单及末尾7条修订优先，历史阶段事实见reports，不在此保留第二套路线。

BASE/EXPECTED_MAIN：62f86cc487af41de7d84bfb5a14d2fb1e0e172ac。INDEX_BUILD_SHA：57864e5dd456251b457238b196e8fed5668d909b（不操作索引机）。仅dev实现并普通push，最终P6_AWAITING_REVIEW。

## 执行已收口（等待最终候选核及提交送审）

真实验收仅 PARTIAL：原三记录任务正式 pause 后 delivered=2、partial export=2；首次 fresh resume HTTP400/CONFIRMED，未定位即额外 resume 的执行偏差产生 operation UNKNOWN=1、pending=2。保原任务及历史，不再请求、不替换、不迁移/清理/退款；不声称完整闭环。

## 已执行范围
- [x] 核源码调用链及平台环境，定义集中CapacityConfig/ResourcePolicy/Workspace持久契约与唯一写入分工。
- [x] Workspace init/inspect：identity、state/tasks/tmp、物理域、锁内policy更新、明确null配额；保持旧P4账本与pending不变。
- [x] 核心接口冻前纠正：新workspace累计quota默认None，legacy原值仅适配器；explicit清单旧4MiB/header64KiB分开，image优先旧8MiB默认；slot/pending/proof与header/RPC真实字段/版本/使用层核，消费者不接未冻schema。
- [x] 有效容量贯通plan/TaskDB重开/CLI/preflight/lane/transport/worker：冻结数量、sample heap与内存、batch、record清单、journal/header/export、image/metadata/chunk。
- [x] 大图片有界chunk流程：同对象/版本/validator、增量SHA、一次原子交付/saved；无整图buffer或整TARfallback。
- [x] CLI/API workspace绑定贯通create/run/pause/cancel/resume/export/doctor/inspect/pubfetch；显式冲突在DB写/网络前拒，默认help/doctor无token读取/网络。
- [x] 相关合成回归：双workspace/同域多进程、近配额/非法config/历史上限计数fixture、旧UNKNOWN不可跨域重试、受影响crash窗口。
- [x] 实际Windows新ownedworkspace正式CLI小metadata pause/resume/export；实际Linux native路径锁小任务；3.10新增点与wheel安装来源/代表任务。
- [x] 已执行一次常规 exit1：36fail/1435pass/36skip/1deselected/252errors；已按失败根因完成定向复测，不宣全套通过。稳定后仅一次常规回归，明确排除stress；Rust有变仅受影响build/test，未变不重复认证；Linux必要nativeworker build。
- [x] README/PROJECT_RULES/P5_ROADMAP/agents统一P5D完成及4/2/2、P4 legacy、默认不hash；包description/version/author/LICENSE/发布CI不动。
- [x] 可选现Publication完整归档：既有gzip基线+两Zstd档本地size/压解成本，不重建/扫描/上传、不宣称未测比率。
- [ ] 待收尾：一份有效报告，正常语义commits显式stage/pushdev核main不动，停止P6_AWAITING_REVIEW。

## Done
- [x] git fetch/status/HEAD/origin dev/main核验一致62f86cc、工作区clean，无reset/rebase或外增量覆盖。
- [x] 读取当前完整执行单及修订、README/pyproject/agents/PROJECT_RULES/旧plan；替换失效旧准备清单。

## 写入分工与接口依赖

- 当前core唯一写方：Workspace/CapacityConfig/ResourcePolicy/BudgetLedger及其新测试；完成后主代理核默认/版本/溢出/持久布局再冻结接口。未冻前消费者不猜字段。
- 主代理：plan、README、agents、PROJECT_RULES、P5_ROADMAP；没有并行文档写方。
- 后续独占任务组：tasks与CLI；独占stream组：publication fetch/session/prepared/transport/production/Rust及相关测试。须等接口冻结再派工，runner/pipeline联动由任务组按stream共享计划集成，禁止两组写同文件。
- sole只读设计/集中候选审；不是新增阶段授权门。平台核：WSL Ubuntu为实际Linux，有cc/gcc及uv；Windows已安装3.10.21供新增兼容专项。

## 不变边界
旧P4不迁移/清理/退款；新workspace非洗旧task UNKNOWN方式。无P2/P3/Publication重建、索引机器操作、上传/visibility变更或下一阶段启动。文档与容量旧数可作默认，不能暗藏不可调cap；实际实现边界须明确拒绝。
