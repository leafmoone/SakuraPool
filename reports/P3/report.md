# SakuraPool P3 — Benchmark Workload Repair 交付报告

**状态：WAITING_REVIEW；CHANGES_REQUIRED 的本轮返修送审，不自行 APPROVED_FOR_MERGE。**

## 版本 / 作用域 / 禁令

| 字段 | 完整 SHA |
|---|---|
| BASE_SHA / main HEAD / origin/main | `6c45548e01b0856d6d4bbce18e240be0b22ae7e7` |
| PREVIOUS_SUBMISSION_SHA | `1150f0f8ec50363c46f525d9a995511f68327395` |
| NEW_IMPLEMENTATION_START_SHA = NEW_IMPLEMENTATION_END_SHA | `c8a286892039acca283a0762df23ac56346f64f4` |
| NEW_SUBMISSION_SHA / dev HEAD / origin/dev | 本报告证据提交（最终答复写完整 SHA；提交后核远端） |

实现只改 `tools/bench_runtime.py` 与 `tests/test_bench_workload.py`；runtime 引擎、P2 契约、query/recovery/32MiB 测试均未改。两来源 `src_a/src_b` 同属 namespace `danbooru`，source ≠ namespace；correctness tests 保留 namespace isolation。pyproject 不含 DuckDB，runtime 源码不导入 DuckDB；架构仍为 Parquet durable source / Roaring set engine / SQLite catalog、bitmap container 与 compiler staging / NumPy mmap locations。未 amend/reset/rebase/force push；main 未改，不开始 P4。只显式暂存本轮实现与报告，旧 root 附件未纳入。

## Workload 契约、分母及实测频率（用户 2–9、27）

`hot_anchor`、`medium_anchor`、`rare_anchor` 对每个已知状态记录分别独立 Bernoulli 采样 70%/10%/0.1%，允许重叠，**不是**互斥 if/elif，也不以 500 个噪声 hot 标签中的单个充当 70% anchor。状态先抽：missing/invalid/empty 不计入 anchor 分母。固定分母：`danbooru tags_state=known records`（不是总 RID、不是 known+empty），每 scale 生成后计算实际 count/frequency 并断言 hot68–72%、medium9–11%、rare0.07–0.13%，不达标直接失败。另保留数百噪声标签。`hot ⊇ corr_a ⊇ corr_b`；`exclude_anchor ⊆ hot`；solo/group 互斥。每 scale generator manifest 与 query JSON 均记录原始统计，query hot cardinality 与生成计数一致。

| scale | frequency denominator | known records | hot count / actual | medium count / actual | rare count / actual |
|---|---|---:|---:|---:|---:|
| 100k | danbooru known | 92,937 | 65,058 / 70.002260% | 9,231 / 9.932535% | 96 / 0.103296% |
| 1M | danbooru known | 929,954 | 650,545 / 69.954535% | 93,005 / 10.001032% | 944 / 0.101510% |
| 5M | danbooru known | 4,649,391 | 3,254,706 / 70.002846% | 464,849 / 9.998062% | 4,728 / 0.101691% |

representative families 共 10 种：source_only = src_a；hot = hot_anchor；rare = rare_anchor；hot_and_medium = 两 anchor AND；three_tag_and = hot AND corr_a AND corr_b；three_tag_or = 三 anchor OR；not = hot AND NOT exclude_anchor；source_2tag = src_b AND hot AND medium；two_source_2tag = (src_a OR src_b) AND hot AND medium；any_of_2 = `(src_b AND hot AND medium) OR (src_a AND hot AND corr_a)`。正式计时前逐一断言 cardinality > 0，any_of 两分支还分别断言 > 0（5M 分支 162,422 和 732,297）。`empty_boundary = solo AND group` 独立断言结果 0，不参加任何 representative latency gate。

## 最终实验方法 / 全量 5M 查询结果（用户 10–12、17–18、28）

环境：Windows 11，pinned Python 3.12.13 / numpy 2.2.6 / pyarrow 18.1.0 / pyroaring 1.1.0。worktree 模式真实模块路径在 `src/sakurapool`，**不是**已安装 wheel 的性能数字；另有 repo 外清 `PYTHONPATH` 的 installed 100k 对照。每阶段子进程隔离，父子模块来源断言。每代表 family 先一次**不计时**完整 plan + 128 locations + materialization warmup，之后分别测 **100 次** warm plan、query→first128 e2e、materialization；所有 100 次 `n=100`。nearest-rank percentile = 排序后 `ceil(p*N)-1`（p50/p95/p99）；单位下表全为 ms，max 为最大值。cold_plan 独立打开快照测。materialization 使用 2000 行真实 batch，恰好达到 10000 不请求额外 batch；`rare` 只返回 4728 行，不冒称 10k，排除 true10k gate，仍如实报告它的材料化时间。

| family | query spec（简写） | count | cold plan | warm plan p50/95/99/max | query→first128 p50/95/99/max | materialization actual/requested；p50/95/99/max |
|---|---|---:|---:|---|---|---|
| source_only | src_a | 2,500,000 | 0.536 | .095/.223/.608/.854 | .085/.181/.388/.618 | 10000/10000；.666/.897/1.023/1.334 |
| hot | hot_anchor | 3,254,706 | .931 | .154/.329/.432/.532 | .157/.255/.309/.356 | 10000/10000；.664/.921/.981/1.036 |
| rare | rare_anchor | 4,728 | .252 | .101/.162/.357/.594 | .099/.163/.225/.236 | **4728/10000**；.356/.602/.911/1.540 |
| hot_and_medium | hot ∧ medium | 325,172 | 2.606 | .475/.710/.814/.921 | .391/.588/.850/1.028 | 10000/10000；.770/1.075/1.292/1.421 |
| three_tag_and | hot ∧ corr_a ∧ corr_b | 659,265 | 3.858 | .412/.668/.798/.961 | .409/.609/.762/.798 | 10000/10000；.674/.833/1.004/1.108 |
| three_tag_or | hot ∨ medium ∨ rare | 3,395,711 | 1.992 | .280/.436/.608/.970 | .294/.409/.473/.483 | 10000/10000；.645/.868/1.032/1.167 |
| not | hot ∧ NOT exclude | 2,604,075 | 4.631 | .368/.753/.823/1.073 | .374/.573/.730/.815 | 10000/10000；.655/.811/.995/1.063 |
| source_2tag | src_b ∧ hot ∧ medium | 162,422 | 3.004 | .323/.550/.865/.917 | .332/.536/.751/.847 | 10000/10000；.679/1.065/1.211/1.310 |
| two_source_2tag | (src_a∨src_b) ∧ hot ∧ medium | 325,172 | 3.494 | .422/.680/.762/.988 | .420/.637/.848/1.179 | 10000/10000；.691/.969/1.172/1.339 |
| any_of_2 | (src_b∧hot∧medium)∨(src_a∧hot∧corr_a) | 894,719 | 5.312 | .664/.972/1.051/1.095 | .670/.953/1.134/1.196 | 10000/10000；.659/.857/.968/1.120 |

**empty_boundary：`danbooru solo AND group` → 0，单独报告、零次 representative 计时。** 5M gates 全 PASS：warm plan count 最大 p95=.972ms ≤200ms；first128 e2e 最大 p95=.953ms ≤500ms；true10k materialization 最大 p95=1.075ms ≤1000ms；locations100k 实际 100,000；缓存 resident 有界。gate 无代表 family、<100 warm 或没有 true10k 时拒绝，而非空集合默认 PASS。

## 编译资源与容量（用户 13–16、19、29）

新 run_fingerprint：`98a2cfed01b625eb1430bf7b668ea7afe31a274a405b0ea638a26b0bb7aade48`（**不同于**旧 `3fbad1ca69afe9fa9c7a03ae10096015270b5f85c1736d66bc3dc2a28dca3519`）。同一新 corpus 上重跑生成、编译、查询；phase JSON 的 run_id 核验均为新指纹，不复用旧基准。

| scale | compile wall s | peak RSS MiB | peak temp MiB | total published snapshot bytes | bytes/sample |
|---|---:|---:|---:|---:|---:|
| 100k | 6.362 | 170.715 | 39.017 | 16,378,810 | 163.7881 |
| 1M | 93.965 | 246.230 | 395.363 | 164,782,368 | 164.782368 |
| 5M | 539.036 | 454.496 | 1,992.538 | 834,043,939 | 166.8087878 |

5M/1M RSS = 1.85×。**5M 完整容量口径**：rid_count=5,000,000；catalog.tags 行数 `unique_tags=758`；`tag_memberships=10,687,187`，即 tag bitmap cardinality 求和、实际 `(namespace,value,rid)` 成员数；`avg_tag_memberships_per_sample=2.1374374`。仅 `kind='tag'` BLOB payload 求和 `tag_bitmap_blob_bytes=11,992,310`；`bytes_per_tag_membership=1.1221203484`。uint32 baseline=`10,687,187*4=42,748,748 bytes`；`bitmap_compression_ratio=11,992,310/42,748,748=0.2805300871`（<1 越小越好）；savings fraction=0.7194699129。**不**把 bitmaps.sqlite 容器文件尺寸冒充 payload。catalog.sqlite=624,750,592；bitmaps.sqlite=14,290,944；locations.npy=195,000,338；附加 marker/manifest=2,065；发布快照总=834,043,939 bytes。尺寸为全发布目录普通文件之和。容量 test 独立打开 SQLite，逐 tag BLOB deserialize 复核 cardinality/长度，P2 Parquet 独立数 known/三 anchor，非 helper 自证。

## 产品验证、轻量预审、差分（用户 20–24）

实现提交先经 reviewer 两次只读轻量预审，第二次 reviewer 独立 `6 passed` + ruff，root 批准启动正式规模（非合并批准）。轻量测试中 40k 两来源语料做 Parquet 独立频率审计、真实 phase_query 100 warm、true10k 与不足10000的小结果实际行数、无额外 batch、empty 分离、p99、容量与 fail-closed gates；将 tiny 结果人为标为 5M 仅为 gate **逻辑单测**，不入正式性能数据。

最终实现树 `c8a286892039acca283a0762df23ac56346f64f4`：

| 命令/范围（精确 argv、cwd、exit、log SHA 见 `bench-repair/commands.json`） | exit | 结果 |
|---|---:|---|
| `uv build --wheel`，全新 Python 3.12.13 venv 正常 wheel+deps 安装，`pip check` | 0 各 | 成功；wheel SHA256 `aa812df01cec291a4d239c41c1a7ec15092e4abd602bdc3e05a9a234fba29306` |
| Git 源码→wheel→site-packages Python 文件路径、字节核验 | 0 | 17 文件；Git vs wheel 仅归一换行，wheel vs install 逐字节；防旧 wheel |
| wheel `python -m pytest tests -q -p no:cacheprovider` | 0 | **191 passed, 1 skipped, 51.71s** |
| host Python 3.14 相同 pytest（仅诊断） | 0 | **191 passed, 1 skipped, 43.00s** |
| `python -m ruff check .` / BASE..HEAD `git diff --check` | 0 / 0 | All checks passed / 无输出 |
| 100k 随机差分 | 0 | 300 specs，**0 mismatches**；未声称 1M/5M 各300 |
| 正式 unit 随机 specs | 0 | `tests/test_runtime_differential.py` `N_SPECS=1000`，包含于完整 pytest |
| installed mode 100k（外部 cwd、无 PYTHONPATH） | 0 | compile 6.278s；diff 300 / 0 mismatches，真实 site-packages 模块 |
| installed CLI：compile、verify --full、query、inspect、lookup | 0 各 | repo 外真实 `python -m sakurapool runtime`，有真实输出 |
| Python 3.10/3.13 wheel install、pip check、site-packages import、verify --full、mini compile/query | 0 各 | 不做跨版本大规模 benchmark；无依赖/API 变更 |

唯一 skip 是 Windows 本机用户无权创建文件 symlink，真实 junction 测试照常运行。合法 2M rid/168 tags/20M memberships/32MiB 驱逐 fixture 随完整 pytest 运行；该 fixture 的安全 fingerprint 绑定 src/合成生成器/Python 依赖，而本次只改 benchmark 工具+其测试，故**可复用旧合法缓存**，不能将复用说成重新从零生成。只有支持版 Python 3.10–3.13 的 pinned wheel 环境作为认证；host 3.14 是诊断。

## 证据、哈希与旧失败保留

`reports/P3/bench-repair/commands.json`：**31 条命令，全部 exit=0**；每条含 argv/cwd/日志 byte length/SHA256。`bench-repair/benchmark.json`、`bench-repair/benchmark-installed.json` 是新指纹的唯一正式基准报告；`reports/P3/hashes.sha256` 按实际 staged Git blobs 计算，报告提交后再次核验。最终答复逐条给出日志字节与哈希。本报告不覆盖此前 raw log、旧 benchmark.json、旧 query-benchmark.json；它们是先前版本的历史证据，不能拿来宣称本轮通过。先前旧 wheel 失败的逐字节归档仍保留在 `final-r4/old-wheel-failure.json`。Git 换行转换可能使 worktree 与提交 blob SHA 不同，证据以提交 blob 为准。

## 边界 / 安全声明

5M **仅授权 synthetic scale**，不推断生产吞吐，不推断 21M 相同延迟。ModelScope **NO**；生产数据/服务访问 **NO**；P4 **NO**；main 修改/合并 **NO**。无 DuckDB；无真实数据服务、生产部署或大规模生产任务；不做引擎替换。此前执行中的禁 amend/reset 偏离已记录在上一轮报告，不能因本轮结果自动抹去或视作获批。本轮只追加新提交，不删除未知文件、不自行批准合并。送审状态 **WAITING_REVIEW**。
