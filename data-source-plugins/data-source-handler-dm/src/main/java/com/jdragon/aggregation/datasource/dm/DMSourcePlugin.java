package com.jdragon.aggregation.datasource.dm;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

@Getter
public class DMSourcePlugin extends RdbmsSourcePlugin {

    private final DataSourceType type = DataSourceType.DM;

    private final String driver = DataSourceType.DM.getDriverClassName();

    private final String jdbc = "jdbc:dm://%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "\"";

    private final String separator = "&";

    private final String extraParameterStart = "?";

    public DMSourcePlugin() {
        super(new DefaultSourceSql());
    }

    @Override
    public String joinJdbcUrl(BaseDataSourceDTO dataSource) {
        String result = String.format(getJdbc(), dataSource.getHost(), dataSource.getPort(), dataSource.getDatabase());
        if (StringUtils.isNotBlank(dataSource.getOther())) {
            Map<String, String> map = JSONObject.parseObject(dataSource.getOther(), new TypeReference<LinkedHashMap<String, String>>() {
            });
            StringBuilder str = new StringBuilder();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key.contains("socketTimeout")) {
                    System.setProperty("DM.jdbc.ReadTimeout", value);
                } else if (key.contains("connectTimeout")) {
                    System.setProperty("DM.net.CONNECT_TIMEOUT", value);
                } else {
                    str.append(String.format("%s=%s%s", key, map.get(key), getSeparator()));
                }
            }
            if (str.length() > 0) {
                str.deleteCharAt(str.length() - 1);
                result += getExtraParameterStart() + str;
            }
        }
        return result;
    }
}
