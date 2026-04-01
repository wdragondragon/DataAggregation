# Transformer迁移映射表

## 映射规则
- **源目录**：`/c/dev/ideaProject/bm/diydataxtransformer/`下的目录
- **目标目录**：下划线风格，与transformer.txt中的名称一致
- **Module名称**：去除`bm_`前缀，保持原命名（与目标目录相同）
- **artifactId**：去除`bm_`前缀（从pom.xml中获取）
- **类名映射**：去除`Bm`前缀，数字开头类名特殊处理（如`Bm3DesStr` → `Des3Str`）

## 完整映射表

| 序号 | Transformer名称 | 源目录 | 目标目录 | Module名称 | ArtifactId映射 | 类名映射 | 特殊依赖 | 状态 |
|------|----------------|--------|----------|------------|----------------|----------|----------|------|
| 1 | range_number_filter | bmrangenumberfilter | range_number_filter | range_number_filter | bm_range_number_filter → range_number_filter | BmRangeNumberFilter → RangeNumberFilter | 无特殊依赖 | ✅ |
| 2 | string_operation_filter | bmstringoperationfilter | string_operation_filter | string_operation_filter | bm_string_operation_filter → string_operation_filter | BmStringOperationFilter → StringOperationFilter | 无特殊依赖 | ✅ |
| 3 | number_operation_filter | bmnumberoperationfilter | number_operation_filter | number_operation_filter | bm_number_operation_filter → number_operation_filter | BmNumberOperationFilter → NumberOperationFilter | 无特殊依赖 | ✅ |
| 4 | date_operation_filter | bmdateoperationfilter | date_operation_filter | date_operation_filter | bm_date_operation_filter → date_operation_filter | BmDateOperationFilter → DateOperationFilter | 无特殊依赖 | ✅ |
| 5 | date_filter | bmdatefilter | date_filter | date_filter | bm_date_filter → date_filter | BmDateFilter → DateFilter | 无特殊依赖 | ✅ |
| 6 | null_value_filter | bmnullvaluefilter | null_value_filter | null_value_filter | bm_null_value_filter → null_value_filter | BmNullValueFilter → NullValueFilter | 无特殊依赖 | ✅ |
| 7 | null_value_replace | bm_null_value_replace | null_value_replace | null_value_replace | bm_null_value_replace → null_value_replace | BmNullValueReplace → NullValueReplace | 无特殊依赖 | ✅ |
| 8 | add_default_value | bmadddefaultvalue | add_default_value | add_default_value | bm_add_default_value → add_default_value | BmAddDefaultValue → AddDefaultValue | 无特殊依赖 | ✅ |
| 9 | insert_sys_time | bminsertsystime | insert_sys_time | insert_sys_time | bm_insert_sys_time → insert_sys_time | BmInsertSysTime → InsertSysTime | 无特殊依赖 | ✅ |
| 10 | date_transformer | bmdatetransformer | date_transformer | date_transformer | bm_date_transformer → date_transformer | BmDateTransformer → DateTransformer | 无特殊依赖 | ✅ |
| 11 | trim_spaces_str | bmtrimspacesstr | trim_spaces_str | trim_spaces_str | bm_trim_spaces_str → trim_spaces_str | BmTrimSpacesStr → TrimSpacesStr | 无特殊依赖 | ✅ |
| 12 | value_filter | bmvaluefilter | value_filter | value_filter | bm_value_filter → value_filter | BmValueFilter → ValueFilter | 需要StringDateFormatEnum | ✅ |
| 13 | replace_str | bmreplacestr | replace_str | replace_str | bm_replace_str → replace_str | BmReplaceStr → ReplaceStr | 需要ValidateUtil | ✅ |
| 14 | underline_toCamel_str | bmunderlinetocamelstr | underline_toCamel_str | underline_toCamel_str | bm_underline_toCamel_str → underline_toCamel_str | BmUnderlineToCamelStr → UnderlineToCamelStr | 无特殊依赖 | ✅ |
| 15 | camel_toUnderline_str | bmcameltounderlinestr | camel_toUnderline_str | camel_toUnderline_str | bm_camel_toUnderline_str → camel_toUnderline_str | BmCamelToUnderlineStr → CamelToUnderlineStr | 无特殊依赖 | ✅ |
| 16 | number_cut | bmnumbercut | number_cut | number_cut | bm_number_cut → number_cut | BmNumberCut → NumberCut | 无特殊依赖 | ✅ |
| 17 | SHA256_str | bmsha256str | SHA256_str | SHA256_str | bm_SHA256_str → SHA256_str | BmSHA256Str → SHA256Str | 无特殊依赖 | ✅ |
| 18 | string_cut | bmstringcut | string_cut | string_cut | bm_string_cut → string_cut | BmStringCut → StringCut | 无特殊依赖 | ✅ |
| 19 | date_mask | bmdatamask | date_mask | date_mask | bm_date_mask → date_mask | BmDataMask → DataMask | 无特殊依赖 | ✅ |
| 20 | MD5_str | bmmd5str | MD5_str | MD5_str | bm_MD5_str → MD5_str | BmMD5Str → MD5Str | commons-codec | ✅ |
| 21 | sm2_str | bmsm2str | sm2_str | sm2_str | bm_sm2_str → sm2_str | BmSm2Str → Sm2Str | 无特殊依赖 | ✅ |
| 22 | rsa_str | bmrsastr | rsa_str | rsa_str | bm_rsa_str → rsa_str | BmRsaStr → RsaStr | 无特殊依赖 | ✅ |
| 23 | idea_str | bmideastr | idea_str | idea_str | bm_idea_str → idea_str | BmIdeaStr → IdeaStr | 无特殊依赖 | ✅ |
| 24 | 3des_str | bm3desstr | 3des_str | 3des_str | bm_3des_str → 3des_str | Bm3DesStr → Des3Str | hutool-crypto | ✅ |
| 25 | des_str | bmdesstr | des_str | des_str | bm_des_str → des_str | BmDesStr → DesStr | hutool-crypto | ✅ |
| 26 | aes_str | bmaesstr | aes_str | aes_str | bm_aes_str → aes_str | BmAesStr → AesStr | hutool-crypto, commons-codec | ✅ |

## 注意项
1. **null_value_replace**：transformer.txt中已修正拼写，源目录为`bm_null_value_replace`，迁移时保持正确拼写
2. **数字开头类名**：`Bm3DesStr` → `Des3Str`，其他类似处理
3. **依赖检查**：加密相关transformer可能依赖hutool-crypto或其他加密库
4. **artifactId确认**：需要检查每个pom.xml中的实际artifactId

## 迁移状态
✅ 所有26个transformer已成功迁移并编译通过

## 注意事项
1. **MD5_str**和**aes_str**需要`commons-codec`依赖，已添加到各自pom.xml
2. **3des_str**、**des_str**、**aes_str**需要`hutool-crypto`依赖，已添加到各自pom.xml
3. **value_filter**和**replace_str**需要公共工具类，已从源目录复制相关文件
4. 所有transformer均已添加到父模块pom.xml的modules列表中

---
**生成时间**：2026-03-26  
**最后更新**：2026-03-26  
**更新说明**：所有26个transformer迁移完成，编译验证通过