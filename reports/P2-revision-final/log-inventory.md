# 本轮原始日志完整清单

cwd: D:/SakuraTool/SakuraPool。最终核心命令 argv/exit 在 manifest.json；初轮在
../P2-revision/manifest.json。报告提交 76627fd517c11053cae85e3aa36cee318391d9a8 后仅追加
日志属性/清单提交，源码未改；.gitattributes 明确 -text -whitespace 保留原始 bytes。

## 最终实现 tree 日志

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
| release-venv | 0 | 201 | f6cf78fd5c11b3f48396d5116e41a84288bd48616f5bf0f4fa269ea304f33a81 |
| release-install | 124 | 151 | e1b5ad922fd158925981c519df45dc8d5175d94579452f4c9dbb2967cb3956a5 |

附加 release 命令（正常依赖解析，无复制/no-deps）：

```console
uv venv --python 3.12 /tmp/sakurapool-p2-release-env
UV_HTTP_TIMEOUT=30 UV_HTTP_RETRIES=0 timeout 180s uv pip install --python /tmp/sakurapool-p2-release-env/Scripts/python.exe D:/tmp/sakurapool-p2-wheel/sakurapool-0.1.0-py3-none-any.whl pip
```

## 初轮日志（reports/P2-revision，不作为最终认证）

| log | exit | bytes | SHA256 |
|---|---:|---:|---|
| git | 0 | 393 | ece589ea048f8ef70f0aa87e8fa040dd971133f84f846652a522f249690f7ea9 |
| environment | 0 | 187 | b5baa9ffcb40dc7fa6071947c2ad36ca49683b62193cd6bfd8b659bc047e01fa |
| pytest | 0 | 5411 | 5c15a812316dfea91b14daf6814ccc403a7b4342c5cb9176706e133d716a0612 |
| ruff | 0 | 19 | 82b3e6a6c090a57601d22943bd23fca9218d1031dbe5a7b754092f9a156b4f18 |
| diff | 0 | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| diff-base | 0 | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| build | 0 | 3740 | ad22c8cd658db7345d92a31b0d7fa649911c99fd41529236eb7e342cfc01ae89 |
| benchmark | 0 | 682 | 7a6e5163bd28481fd254226ae431ff2d59ee111f90bbb7082e3f7784b4a1617d |
| extents | 0 | 1560 | c9bd4671d8b2847e214c549b515d7e7efc2984d9e244f0fe981f04054aa65d39 |
| venv | 0 | 217 | 846c220433e89fae5876add4cece3778aa2bf0b0857be0ee26de41fe25cc2fd3 |
| wheel-install-attempt | 124 | 265 | d21e74aa3af3f64b0abdf9c44d2d67f59acac6f42be04e51118893e80b392534 |

最后一项命令：`timeout 45s python -m pip --python C:/Users/PC/AppData/Local/Temp/sakurapool-p2-env/Scripts/python.exe install --timeout 10 --retries 0 D:/tmp/sakurapool-p2-wheel/sakurapool-0.1.0-py3-none-any.whl`。
其45s外层超时为124，不等于 pip 自身成功/失败结论。
