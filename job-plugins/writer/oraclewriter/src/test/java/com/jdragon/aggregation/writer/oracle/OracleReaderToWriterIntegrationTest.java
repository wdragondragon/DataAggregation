package com.jdragon.aggregation.writer.oracle;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.oracle.OracleSourcePlugin;
import com.jdragon.aggregation.oracle.OracleReader;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OracleReaderToWriterIntegrationTest {

    private static final String SOURCE_TABLE = "TEST_ETL_SOURCE";
    private static final String TARGET_TABLE = "TEST_ETL_TARGET";

    private final OracleSourcePlugin plugin = new OracleSourcePlugin();
    private BaseDataSourceDTO dataSource;

    @BeforeClass
    public static void initHome() {
        String home = "E:\\work\\DataAggregation\\package_all\\aggregation";
        System.setProperty("aggregation.home", home);
        SystemConstants.HOME = home;
        SystemConstants.CORE_CONFIG = home + "\\conf\\core.json";
    }

    @Before
    public void setUp() {
        dataSource = buildDataSource();
        dropTableSafe(SOURCE_TABLE);
        dropTableSafe(TARGET_TABLE);

        plugin.executeUpdate(dataSource, "CREATE TABLE " + SOURCE_TABLE + " ("
                + "id NUMBER PRIMARY KEY,"
                + "name VARCHAR2(100),"
                + "amount NUMBER(10,2)"
                + ")");
        plugin.executeUpdate(dataSource, "CREATE TABLE " + TARGET_TABLE + " ("
                + "id NUMBER PRIMARY KEY,"
                + "name VARCHAR2(100),"
                + "amount NUMBER(10,2)"
                + ")");

        plugin.executeUpdate(dataSource,
                "INSERT INTO " + SOURCE_TABLE + " (id, name, amount) VALUES (1, 'alpha', 12.50)");
        plugin.executeUpdate(dataSource,
                "INSERT INTO " + SOURCE_TABLE + " (id, name, amount) VALUES (2, 'beta', 24.75)");
        plugin.executeUpdate(dataSource,
                "INSERT INTO " + SOURCE_TABLE + " (id, name, amount) VALUES (3, 'gamma', 99.01)");
    }

    @After
    public void tearDown() {
        dropTableSafe(SOURCE_TABLE);
        dropTableSafe(TARGET_TABLE);
    }

    @Test
    public void shouldCopyOracleTableViaJobContainer() {
        // Verify source table has data
        List<Map<String, Object>> sourceRows = plugin.executeQuerySql(
                dataSource, "SELECT id, name, amount FROM " + SOURCE_TABLE + " ORDER BY id", true)
                .getBodies();
        System.out.println("=== SOURCE TABLE ROWS: " + sourceRows.size() + " ===");
        for (Map<String, Object> row : sourceRows) {
            System.out.println("  " + row);
        }

        Map<String, Object> jobConfig = buildJobConfig();
        System.out.println("=== JOB CONFIG ===");
        System.out.println(Configuration.from(jobConfig).beautify());

        JobContainer container = new JobContainer(Configuration.from(jobConfig));
        container.addConsumerPlugin(PluginType.READER, new OracleReader());
        container.addConsumerPlugin(PluginType.WRITER, new OracleWriter());
        container.start();

        List<Map<String, Object>> targetRows = plugin.executeQuerySql(
                dataSource, "SELECT id, name, amount FROM " + TARGET_TABLE + " ORDER BY id", true)
                .getBodies();

        System.out.println("=== TARGET TABLE ROWS: " + targetRows.size() + " ===");
        for (Map<String, Object> row : targetRows) {
            System.out.println("  " + row);
        }

        assertNotNull("target rows should not be null", targetRows);
        assertEquals("target table should have 3 rows", 3, targetRows.size());

        assertEquals((Object) 1L, toLong(targetRows.get(0).get("ID")));
        assertEquals("alpha", targetRows.get(0).get("NAME"));
        assertEquals(Double.valueOf(12.50), toDouble(targetRows.get(0).get("AMOUNT")));

        assertEquals((Object) 2L, toLong(targetRows.get(1).get("ID")));
        assertEquals("beta", targetRows.get(1).get("NAME"));
        assertEquals(Double.valueOf(24.75), toDouble(targetRows.get(1).get("AMOUNT")));

        assertEquals((Object) 3L, toLong(targetRows.get(2).get("ID")));
        assertEquals("gamma", targetRows.get(2).get("NAME"));
        assertEquals(Double.valueOf(99.01), toDouble(targetRows.get(2).get("AMOUNT")));
    }

    private Map<String, Object> buildJobConfig() {
        Map<String, Object> connect = new LinkedHashMap<>();
        connect.put("host", dataSource.getHost());
        connect.put("port", String.valueOf(dataSource.getPort()));
        connect.put("database", dataSource.getDatabase());
        connect.put("userName", dataSource.getUserName());
        connect.put("password", dataSource.getPassword());
        connect.put("usePool", true);

        Map<String, Object> readerConfig = new LinkedHashMap<>();
        readerConfig.put("connect", connect);
        readerConfig.put("table", SOURCE_TABLE);
        readerConfig.put("columns", List.of("id", "name", "amount"));

        Map<String, Object> reader = new LinkedHashMap<>();
        reader.put("type", "oracle");
        reader.put("config", readerConfig);

        Map<String, Object> writerConfig = new LinkedHashMap<>();
        writerConfig.put("connect", connect);
        writerConfig.put("table", TARGET_TABLE);
        writerConfig.put("columns", List.of("id", "name", "amount"));

        Map<String, Object> writer = new LinkedHashMap<>();
        writer.put("type", "oracle");
        writer.put("config", writerConfig);

        Map<String, Object> job = new LinkedHashMap<>();
        job.put("reader", reader);
        job.put("writer", writer);
        job.put("jobId", System.currentTimeMillis());
        return job;
    }

    private BaseDataSourceDTO buildDataSource() {
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        dto.setHost("localhost");
        dto.setPort("1521");
        dto.setDatabase("orcl");
        dto.setUserName("system");
        dto.setPassword("oracle");
        dto.setType("oracle");
        dto.setUsePool(false);
        return dto;
    }

    private void dropTableSafe(String tableName) {
        try {
            plugin.executeUpdate(dataSource,
                    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tableName + "'; EXCEPTION WHEN OTHERS THEN NULL; END;");
        } catch (Exception ignored) {
        }
    }

    private Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Double) return (Double) value;
        return Double.valueOf(value.toString());
    }

    private Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.valueOf(value.toString());
    }
}
