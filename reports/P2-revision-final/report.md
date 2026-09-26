# P2 审查缺口修复报告（WAITING_REVIEW，安装 gate 未通过）

## 版本与实际结论

BASE/main 固定 `52358d6fca728d2bba12814490e0974a6907b218`。
接管 dev `8e339ad3eda9ae03ace70869c819ef5082cf938e`，工作树干净，与用户指定一致。
本轮实现提交 `22bf9fa1947d4bb1aeda2a046591f9f993a51492`；验证脚本修复暨最终
IMPLEMENTATION `26328c0003920192d47dfa5cbe53b1781b849f89`。
SUBMISSION 是随后仅报告/原始日志提交，完整 SHA 在最终回复中提供，避免自引用。

BASE 之后全部中间提交：
- c480253c08b4e4547f9210025ee68f4f5f1a55ad：历史初始 P2 实现。
- c705b7603610ca613fe51f01381f5cf9104e4c17：历史 P2 报告。
- 8e339ad3eda9ae03ace70869c819ef5082cf938e：历史日志字节修复。
- 22bf9fa1947d4bb1aeda2a046591f9f993a51492：本轮身份、四表、配对、事务与 CLI 修复。
- 26328c0003920192d47dfa5cbe53b1781b849f89：Windows wheel 路径与验证子进程超时修复。

只 dev 追加；未 amend/reset/merge/main/push/P3。提交前检查身份、分支、diff、显式路径 staging。
本轮不是只改报告：修改 indexer/registry/cli、新增 records，更新原 fixture 测试、补充边界测试。
最终实现 tree 在 Python 3.14.5/Arrow 25.0.1 上 **65 tests 全通过**，ruff 全仓通过，
工作树及 BASE..HEAD diff-check 通过，隔离 build wheel 通过。**不等于 pinned 环境通过**。

Python 3.12.13 已通过 uv 安装并可运行；正常 wheel 依赖解析成功，但 PyArrow 18.1.0
下载持续超时，故 clean install、该环境全测试、pip check、installed-wheel CLI 三项 smoke
仍未完成。不是权限不足，也不是用户应授权的等待；是明确保留的下载/环境 gate 失败。
状态 WAITING_REVIEW，不宣称全部 P2 验收完成。

## P1_CONTRACT_CHANGE

原定义：P1 只有 `DEFAULT_SCHEMA(id,value,label)` 和查询/模型注册，并没有完整 domain
RecordKey/ObjectRef/MemberRef/tag 契约。历史 P2 把每 image occurrence 当 Object，使用
SHA256(dataset/source/shard/member/offset/size)，samples 与 objects 一对一。
问题：TAR 与记录层混淆、offset 改变身份、无 JSON extent、tag 没有 namespace/origin/category。
新定义：TAR shard 是 Object；object_id 是 dataset 内 canonical relative POSIX TAR path；
sample_path 是 adapter 的完整 logical stem；保留三个原字段并使用精确 BLAKE2b16：

```python
hashlib.blake2b(json.dumps(
    ["sakurapool-record-v1", dataset_id, object_id, sample_path],
    ensure_ascii=False, separators=(",", ":")
).encode("utf-8"), digest_size=16).hexdigest()
```

非规范 object_id 拒绝，不猜测归一化。ObjectRef 与两种 MemberRef 可构造。
迁移：on-disk schema 升为 2，旧 output 不可原地复用；必须新目录重扫；旧 IDs、列名、
错误码、summary 与 crash hooks 均有破坏性更正。P1 toy query API 保留，但不伪称其为
四表 domain 查询层。tags 改为 list<struct<value,namespace,origin,category>>；annotation
是每 metadata 文档一行，而非每 tag 一行。README 明确完整新契约。

## 原 P2 gates 逐项映射

| Gate | 实现与结果 |
|---|---|
| TAR Object / 多物理 samples | objects 每 shard 一行；A 一 object 两 samples；benchmark 一 object 一万 samples |
| 精确 RecordKey/BLAKE2b16/保留字段 | records.py，中文 UTF8 固定公式测试；模拟碰撞不覆盖，scan 不发布冲突 shard |
| source registry | adapter.source 写四表，不从路径猜；G source mismatch |
| logical/build path 分离 | objects.path/object_id 仅相对 TAR path；local Path 仅用于本地 I/O |
| validator 强度 | version=1，strong:sha256；size/mtime 只是变化提示；同大小同 mtime 内容变化拒绝 |
| 四 Arrow 表/P1不足 | 新 schema v2，README P1_CONTRACT_CHANGE；四表含空表 schema roundtrip |
| tag namespace/origin/category | samples list struct；known/empty/missing/invalid；声明 origin 不叫 computed |
| metadata nullable/required | adapter metadata_required 默认 true；false 时无 metadata 样本仍写入且 JSON extent null |
| uint64/no decode | image/JSON offsets/length uint64；2^63+1/2 Parquet roundtrip；无 decoder |
| ObjectRef+MemberRef | records.py；fixture 构造 ObjectRef 和 image/JSON refs |
| CLI/config/version | sakura alias、--version、config validate、命名四参数；source smoke 测试；安装 smoke 未完成 |
| 单 TAR/recursive *.tar | discovery 支持两者；压缩 magic 和扩展检测；伪装 gzip/bzip2/xz 测试 |
| summary/非零 | seen/committed/skipped 全按 shards；samples 物理行数；row errors=1，fatal=2 |
| logical stem/adapter | full-path 默认；角色 prefix 明确跨 images/meta；拒绝 image/metadata 歧义 |
| 辅助项 | README.md/manifest.json/foo.bin/hidden/目录忽略；H 零错误 |
| 错误词汇 | 十个原要求代码均定义；archive/conflict fatal，其他写 errors；明确 CORRUPT_COMMIT |
| A–H 精确 fixture | 下表，已通过，不再使用旧自定义 A–H 替代 |
| image/JSON indexed seek | 所有正常 A/F 两角色与 tarfile/original payload 三方相等；下方六实测 extent |
| crash A–E | 原定义 hook+异常与真实 os._exit(73)；A–D 重扫，E 验 hash skip |
| 四表分批 Parquet | ParquetWriter；每 shard 四表、空表 schema；内存仍按 shard，不声称流式有界 |
| fsync/rename/COMMIT | writable file flush/fsync close；四 finals 后 marker；POSIX dir fsync，Windows 无断电保证 |
| marker 内容 | path/bytes/SHA256/rows、input validator、schema/builder/dataset/object/created_at |
| partial ownership | 只清理当前已注册 shard reserved partial；用户 user.partial 原字节保留 |
| corrupt commit | 五 artifact 破坏及可读 Parquet 修改均失败，不重建；CLI fatal 非零 |
| input change | content/touch/add/remove/config/scan 中修改均拒绝；保留旧 offset 不信任 |
| hash 声明/computed | 默认无 image 单独读取；可选 image SHA；whole-TAR 强 hash 多遍读取如实披露 |
| fresh process/logic determinism | 两全新进程四 schema 验证，四表逻辑内容一致、ID/counts一致；时间戳不承诺相同 |
| >2^32 / conflict | >2^63 uint64 往返；模拟 digest 冲突不覆盖不提交 |
| 10k benchmark | header/json/parquet/total/RSS 实测；不生产吞吐声称 |
| 全测试/ruff/diff/build | 实现 tree 全通过，但 Python3.14/Arrow25 是非支持环境诊断 |
| clean venv/pins/pip check/smoke | Python3.12 正常解析安装未完成，下载超时；明确 FAIL/NOT RUN |

错误枚举：invalid_post_id、missing_image、missing_metadata、metadata_invalid、source_mismatch、
ambiguous_image_member、ambiguous_metadata_member、unsupported_member、unsupported_archive、
record_identity_conflict。损坏 commit 的 fatal 名称为 CORRUPT_COMMIT。

## A–H 期望/实际

每个 fixture 都是一 TAR Object。invalid/unpaired 候选记 errors，不计成功 samples。

| Fixture | 精确输入 | 期望 samples/errors | 实际 |
|---|---|---|---|
| A | 1.jpg+1.json+2.webp+2.json | 2 / 无 | 一致 |
| B | 3.jpg | 0 / missing_metadata | 一致 |
| C | 4.json | 0 / missing_image | 一致 |
| D | 5.jpg+5.webp+5.json | 0 / ambiguous_image_member | 一致 |
| E | 6.jpg+invalidJSON 6.json | 0 / metadata_invalid | 一致 |
| F | images/7.jpg+meta/7.json，role prefixes | 1 / 无 | 一致 |
| G | registry A + metadata source B | 0 / source_mismatch | 一致 |
| H | README.md+manifest.json+foo.bin（另含 hidden） | 0 / 无 | 一致 |

## Crash A–E 期望/实际

| 阶段 | 中断点 | partial/final/marker 实测数 | restart |
|---|---|---|---|
| A | samples partial 写一半，未完整 footer | 2/0/0 | 重扫，samples=2、skipped=0 |
| B | 四 Parquet 已 close/fsync，未 rename | 4/0/0 | 同上 |
| C | 一个 final rename，没 marker | 3/1/0 | 同上 |
| D | 四 final，没 marker | 0/4/0 | 同上 |
| E | marker 已发布 | 0/4/1 | hashes 校验后 skipped=1、samples=2 |

异常 hook 与真实子进程终止都通过。A 的异常路径 context manager 会在展开时写 footer，
真实 os._exit 测试覆盖不执行 Python 清理的情形。未知 user.partial 未删除。

## 实际 extent 回读（全部 seek == tarfile.extractfile == 原 payload）

| member | offset | length | SHA256 |
|---|---:|---:|---|
| 1.jpg | 512 | 3 | 7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed |
| 1.json | 1536 | 14 | e287f621910b125929a8391d015b047588d3eb8cf627d35db35c057d00643073 |
| 2.webp | 2560 | 3 | 3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3 |
| 2.json | 3584 | 11 | b35b1ec1c0c72c4bbd16bd9d6c2cbcac8224272cd7e6ecf504a78f2c7e989b2a |
| images/7.jpg | 4608 | 5 | 3ba8d02b16fd2a01c1a8ba1a1f036d7ce386ed953696fa57331c2ac48a80b255 |
| meta/7.json | 5632 | 2 | 44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a |

## 环境、命令、退出码、benchmark

cwd 均 `D:/SakuraTool/SakuraPool`，除未来 installed smoke 应在临时目录（本轮未运行）。
Python 3.14.5 / Arrow25.0.1 / pytest9.0.3 / ruff0.16.1 / build1.2.2.post1，Windows 11。
pyproject 保留 Arrow18.1.0/dev pins，补充 requires-python `<3.14`，没有抹掉 pin 失败。
支持范围声明为 3.10–3.13，但本轮 pinned 兼容测试尚未成功，不能声称已认证该范围。

最终 IMPLEMENTATION 后：
- `PYTHONPATH=src python -m pytest -q -rA`：0，65 passed。
- `python -m ruff check .`：0。
- `git diff --check`：0。
- `git diff 52358d6fca728d2bba12814490e0974a6907b218..HEAD --check`：0。
- `python -m build --wheel --outdir D:/tmp/sakurapool-p2-wheel`：0，隔离安装 setuptools75.6.0。
  Windows Python 中 `/tmp/sakurapool-p2-wheel` resolve 为 D:/tmp；初轮 shell MSYS 路径曾不同，
  已通过第二实现提交固定 build/install 使用同一 resolved path 并重跑全部验证。
- `PYTHONPATH=src python tools/benchmark_indexer.py`：0。
- `PYTHONPATH=src python -c "from tools.p2_extent_probe import main;main()"`：0。
- `uv venv --python 3.12 <temporary>/venv`：0，Python3.12.13。
- `uv pip install --python <venv>/Scripts/python.exe D:/tmp/sakurapool-p2-wheel/sakurapool-0.1.0-py3-none-any.whl pip pytest==8.3.4 ruff==0.9.2 build==1.2.2.post1`：124，90s 超时。
  正常解析 11 包；pip 已下载，ruff/pyarrow 下载未完成。随后仅 wheel+pip 的180s尝试亦124，
  解析3包后卡在 Arrow 下载。未使用 --no-deps、不复制 Arrow。
- pinned pytest/ruff、pip check、installed sakura --version/config validate/index scan：NOT RUN。
- verifier 总退出1，正确保留安装失败，不再把 failures 空列表伪称成功。

10k synthetic **1 shard / 10000 samples**：header 0.23334309994243085s，JSON
0.05724639864638448s，Parquet 0.072721500066109s，total 0.7518633999861777s，
peak RSS 90,820,608 bytes。RSS 包括 imports/fixture generation，total 不含造 fixture，含全 TAR
hash；仅本机非支持运行环境的合成观察，无生产吞吐/内存上界承诺。

早期失败如实保留：初轮 ruff 四处 E501（已修复）；uv 安装 setuptools/镜像 build 下载超时；
bootstrap pip/ensurepip 工具超时；第一次 verifier 外层240s超时。早期 reports/P2-revision
日志不覆写；它不是最终验证证据。最初工具错误没有完整 byte log，不能补造 SHA；本轮最终
manifest 和附加日志才有可复核 bytes/SHA。历史 reports/P2 是 v1，不适用于本轮 gate。

## 最终运行日志哈希表

| log | exit | bytes | SHA256 |
|---|---:|---:|---|
| git | 0 | 505 | 81c0d83b6b378461dba48d99ffd65016c23f6a33ceeecdd8010e2874c45e8a12 |
| environment | 0 | 187 | b5baa9ffcb40dc7fa6071947c2ad36ca49683b62193cd6bfd8b659bc047e01fa |
| pytest | 0 | 5412 | ec2c8d9d33f2ff03a2140b672e6d3c721b35ce1c053f354fac36846dd2c43f23 |
| ruff | 0 | 19 | 82b3e6a6c090a57601d22943bd23fca9218d1031dbe5a7b754092f9a156b4f18 |
| diff | 0 | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| diff-base | 0 | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| build | 0 | 3740 | 38d28a8a2f0e754049fbbf6389040a1e500ed87ceefc6402f50d510d4558cd68 |
| benchmark | 0 | 679 | ed38fc49cbdf518fc3db92ab140690665aedc56df573ae338366a2058d407996 |
| extents | 0 | 1560 | c9b99c10e271a0e6ff84f57897fe77a7a4b6966a8ec789d0e6e1adc3ee9bcb64 |
| venv | 0 | 217 | 6543ab9d4e9e136da10568269e81bd117fac640742ec3af7979a26b329f832a8 |
| wheel-install | 124 | 247 | d26d6f82e1057916e1f7168c7a7f0e6154be8c82eb060908066e1ebeea133cfb |
| status | 0 | 61 | 362be672c796139142ae8b5113f9a602ad646f97d2f0867fdbdbde1b3b58f198 |

所有本轮保留日志（包括初轮、附加失败）完整 bytes/SHA 清单在随提交的 log-inventory；
上述为最后实现 tree 的主要命令全表。manifest 逐命令 argv/cwd/exit 可直接复核。

## 限制、安全与待审

1. pinned clean install 和 wheel smoke 未完成，是阻止全部 gate PASS 的真实剩余项。
2. Windows 无目录 fsync/断电保证；POSIX 分支未在本机运行；只能声称进程崩溃恢复。
3. 单 writer、静止输入前提；全 TAR hash 检测已观察到的变化，不防恶意并发 racing writer。
4. COMMIT hashes 防意外损坏，不是认证签名；INPUT/文件命名空间保留给构建器。
5. 内存与最大 shard 成正比；第一 samples 分块为一半，其余1024行批次，不是全程有界 streaming。
6. sample_path 解释为 adapter logical stem，object_id 为相对 shard path；需 reviewer 审阅该具体迁移。
7. 本轮未访问数据服务、未泄露凭证；只下载测试依赖/兼容Python；无图像 decode、无P3。
8. 新报告提交只含证据，不改已测试源码；最终 dev/main/status/SUBMISSION/报告 SHA 在最终回复核验。

**WAITING_REVIEW；未合并。**
