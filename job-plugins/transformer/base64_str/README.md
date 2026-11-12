# Base64 文本字段转换器

`base64_str` 在“规整”分类下提供 Base64 编码和解码，可绑定采集任务的字段转换链。Base64 是编码方式，不提供加密或脱敏保护。

## 参数

运行配置中的 `parameter.columnIndex` 是从 0 开始的字段下标。`parameter.paras` 按顺序传递下列参数，框架会自动在插件调用参数前插入 `columnIndex`。

| 顺序 | 参数 | 取值 | 默认值 |
| --- | --- | --- | --- |
| 1 | `option` | 必填，`encode` 编码或 `decode` 解码 | 无 |
| 2 | `variant` | `standard` 或 `url_safe` | `standard` |
| 3 | `charset` | 当前 JDK 支持的字符集名称或别名，例如 `UTF-8`、`GBK` | `UTF-8` |

可选参数省略、`null` 或空白时使用默认值；配置参数会去除首尾空白。`option` 和 `variant` 的取值区分大小写。需要指定字符集同时保留默认字母表时，传入 `["encode", "", "GBK"]`。

```json
{
  "name": "base64_str",
  "parameter": {
    "columnIndex": 1,
    "paras": ["encode", "standard", "UTF-8"]
  }
}
```

将 `option` 改为 `decode` 即可解码。UTF-8 编码的 `中文` 转换为 `5Lit5paH`；GBK 编码则为 `1tDOxA==`。`url_safe` 使用 `-` 和 `_` 替换 `+` 和 `/`。编码保留 `=` 填充且不插入换行；解码沿用 JDK Base64 Decoder 语义，接受合法的无填充尾组，拒绝错误填充、非法字符、换行和混用另一种字母表。

## 字段和错误行为

- 只替换所选列，结果类型为 `StringColumn`，其他列不变；多个 transformer 按已有字段转换链顺序执行。
- `null` 字段原样保留；空字符串转为空字符串；纯空白作为实际文本参与编码，不会被修剪或跳过。
- 非 `BYTES` 字段按现有 `Column.asString()` 取文本。`BYTES` 的非 null 值（含空 byte 数组）不支持，避免二进制内容经隐式字符集转换而损坏；null `BYTES` 原样保留。
- 文本转字节、字节还原文本均严格使用所选字符集。不可映射字符、非法 Unicode、无法解码的字节均报错，不使用替代字符。
- 缺少操作、参数类型错误、未知操作/字母表、不支持的字符集和非法下标使用核心 `TRANSFORMER_ILLEGAL_PARAMETER`；数据转换失败使用 `TRANSFORMER_RUN_EXCEPTION`。
- 采集运行时沿用框架脏数据处理：转换失败的整行不写入目标，记入 transformer 失败/脏数据统计，任务最终状态沿用现有执行引擎规则。

## 构建、交付与验证

使用 JDK 17 和 Maven 3.6.3 或更高版本，在 DataAggregation 仓库运行：

```shell
mvn -pl job-plugins/transformer/base64_str -am install -DskipTests
mvn -f job-plugins/transformer/base64_str/pom.xml verify
```

第二条命令运行单元测试和打包后集成测试。集成测试从 `target/aggregation/plugin/transformer/base64_str` 实际插件 jar 动态加载，检查批采集、流式微批、转换链和脏数据统计。

交付物为 `target/aggregation/plugin/transformer/base64_str` 和 `target/base64_str.zip`。插件包只含实现 jar、元数据、配置模板和说明；依赖复用运行时已有 core/commons，无新增运行依赖。通过现有插件仓库发布、Worker 插件解析和 `packe_move_transformer.sh` 收集机制交付，所有实际执行转换的 Worker 均需可解析该插件。
