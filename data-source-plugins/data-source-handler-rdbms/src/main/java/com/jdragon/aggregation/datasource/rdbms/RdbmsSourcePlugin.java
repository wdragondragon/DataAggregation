package com.jdragon.aggregation.datasource.rdbms;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class RdbmsSourcePlugin extends AbstractDataSourcePlugin implements RdbmsSourceDefine {

    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        Connection connection;
        try {
            String jdbcUrl = joinJdbcUrl(dataSource);
            if (log.isDebugEnabled()) {
                log.debug("start get {} connection, jdbcUrl={}", getType(), jdbcUrl);
            }
            if (dataSource.isUsePool()) {
                connection = DatasourceCache.getConnection(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword(), getTestQuery());
            } else {
                connection = JdbcSchema.dataSource(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword()).getConnection();
            }
            if (log.isDebugEnabled()) {
                log.debug("connection：{}", connection);
            }
        } catch (Exception e) {
            log.error("get connection fail", e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return connection;
    }

    @Override
    public String joinJdbcUrl(BaseDataSourceDTO dataSource) {
        String result = String.format(getJdbc(), dataSource.getHost(), dataSource.getPort(), dataSource.getDatabase());
        if (StringUtils.isNotBlank(dataSource.getOther())) {
            Map<String, String> map = JSONObject.parseObject(dataSource.getOther(), new TypeReference<LinkedHashMap<String, String>>() {
            });
            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                StringBuilder str = new StringBuilder();
                for (String key : keys) {
                    str.append(String.format("%s=%s%s", key, map.get(key), getSeparator()));
                }
                str.deleteCharAt(str.length() - 1);
                result += getExtraParameterStart() + str;
            }

        }
        return result;
    }
}
