package com.jdragon.aggregation.writer.mysql8;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Mysql8WriterTest {

    @Test
    public void insertWriteModeShouldNotGenerateDuplicateKeyUpdateSql() throws Exception {
        Mysql8Writer writer = initializedWriter("insert");

        String sql = insertSql(writer);

        assertTrue(sql.startsWith("insert INTO customer_order"));
        assertFalse(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    public void updateWriteModeShouldGenerateDuplicateKeyUpdateSql() throws Exception {
        Mysql8Writer writer = initializedWriter("update");

        String sql = insertSql(writer);

        assertTrue(sql.startsWith("INSERT INTO customer_order"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    private Mysql8Writer initializedWriter(String writeMode) {
        Mysql8Writer writer = new Mysql8Writer();
        writer.setPluginJobConf(Configuration.from(config(writeMode)));
        writer.init();
        return writer;
    }

    private Map<String, Object> config(String writeMode) {
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        Map<String, Object> connect = new LinkedHashMap<String, Object>();
        connect.put("host", "127.0.0.1");
        connect.put("port", "3306");
        connect.put("database", "studio_longterm_regression");
        connect.put("userName", "root");
        connect.put("password", "test");
        config.put("connect", connect);
        config.put("table", "customer_order");
        config.put("columns", Arrays.asList("order_id", "customer_name"));
        config.put("pkColumn", Collections.singletonList("order_id"));
        config.put("writeMode", writeMode);
        return config;
    }

    private String insertSql(Mysql8Writer writer) throws Exception {
        Field field = CommonRdbmsWriter.class.getDeclaredField("insertSql");
        field.setAccessible(true);
        return (String) field.get(writer);
    }
}
