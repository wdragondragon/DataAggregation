package com.jdragon.aggregation.odps;

import com.jdragon.aggregation.commons.element.BoolColumn;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.odps.OdpsUtils;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import org.junit.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class OdpsReaderTest {

    private static final LocalDateTime EXECUTION_TIME = LocalDateTime.of(2026, 11, 12, 10, 0, 0);

    @Test
    public void resolveReadModeShouldUseSqlWhenAutoHasSelectSql() {
        assertEquals(OdpsReader.READ_MODE_SQL, OdpsReader.resolveReadMode("auto", "select * from t"));
    }

    @Test
    public void resolveReadModeShouldUseTunnelByDefault() {
        assertEquals(OdpsReader.READ_MODE_TUNNEL, OdpsReader.resolveReadMode("auto", ""));
        assertEquals(OdpsReader.READ_MODE_SQL, OdpsReader.resolveReadMode(null, "select * from t"));
    }

    @Test
    public void resolveReadModeShouldUseSqlOnlyWhenExplicitSqlMode() {
        assertEquals(OdpsReader.READ_MODE_SQL, OdpsReader.resolveReadMode("sql", "select * from t"));
    }

    @Test
    public void normalizeSelectSqlShouldAppendSingleTrailingSemicolon() {
        assertEquals("select * from t;", OdpsReader.normalizeSelectSql(" select * from t "));
        assertEquals("select * from t;", OdpsReader.normalizeSelectSql("select * from t;;"));
    }

    @Test
    public void resolveSelectSqlShouldUseConfiguredSqlWhenPresent() {
        assertEquals("select * from t;", OdpsReader.resolveSelectSql("select * from t", "ignored", Arrays.asList("id"), null));
    }

    @Test
    public void customSqlShouldResolveFunctionsWithOneTimeAndIgnoreUnusedPartition() throws Exception {
        String sql = " select * from orders where dt = '$getCurrentTime(\"yyyyMMdd\", \"-1d\")'"
                + " and today = '$getCurrentTime(\"yyyyMMdd\")' and region = '$toLower(\"CN\")'"
                + " and created_at < GETDATE();; ";
        for (String mode : Arrays.asList("sql", "auto")) {
            Configuration config = readerConfig(mode, sql, "dt='$unknown(\"ignored\")'");
            OdpsReader reader = initializedReader(config);
            reader.prepare(EXECUTION_TIME);

            assertEquals("sql", readerField(reader, "readMode"));
            assertEquals("select * from orders where dt = '20261111' and today = '20261112'"
                    + " and region = 'cn' and created_at < GETDATE();", readerField(reader, "selectSql"));
            assertEquals(sql, config.getString("selectSql"));
        }
    }

    @Test
    public void generatedSqlShouldResolvePartitionsBeforeSplittingAndEscapeValues() throws Exception {
        Configuration config = readerConfig("sql", "", "dt='$getCurrentTime(\"yyyyMMdd\", \"-1d\")',"
                + "today='$getCurrentTime(\"yyyyMMdd\")',region='$toLower(\"O'NEIL\")'");
        config.set("columns", Arrays.asList("id", "name"));
        OdpsReader reader = initializedReader(config);
        reader.prepare(EXECUTION_TIME);

        assertEquals("select id, name from orders where dt = '20261111' and today = '20261112'"
                + " and region = 'o''neil';", readerField(reader, "selectSql"));
    }

    @Test
    public void invalidFunctionsShouldFailPreparationWithFieldNameAndCause() {
        for (String expression : Arrays.asList("$unknown(\"x\")", "$getCurrentTime(\"yyyyMMdd\", \"-xd\")",
                "$getCurrentTime(\"yyyyMMdd\"", "$toLower(${region})")) {
            for (String mode : Arrays.asList("tunnel", "sql")) {
                for (boolean customSql : Arrays.asList(false, true)) {
                    if (customSql && "tunnel".equals(mode)) {
                        continue;
                    }
                    String fieldName = customSql ? "selectSql" : "partitionSpec";
                    Configuration config = readerConfig(mode,
                            customSql ? "select * from orders where dt = '" + expression + "'" : "",
                            customSql ? "" : "dt='" + expression + "'");
                    try {
                        initializedReader(config).prepare(EXECUTION_TIME);
                        fail("Expected invalid expression in " + fieldName + ": " + expression);
                    } catch (IllegalArgumentException expected) {
                        assertTrue(expected.getMessage().contains(fieldName));
                        assertNotNull(expected.getCause());
                    }
                }
            }
        }
    }

    @Test
    public void eachExecutionShouldResolveOriginalConfigurationAgain() throws Exception {
        for (String mode : Arrays.asList("tunnel", "sql")) {
            String partition = "dt='$getCurrentTime(\"yyyyMMdd\", \"-1d\")'";
            Configuration config = readerConfig(mode, "", partition);
            OdpsReader first = initializedReader(config);
            first.prepare(EXECUTION_TIME);
            assertEquals("dt='20261111'", readerField(first, "partitionSpec"));
            assertEquals(partition, config.getString("partitionSpec"));

            OdpsReader next = initializedReader(config);
            next.prepare(EXECUTION_TIME.plusDays(1));
            assertEquals("dt='20261112'", readerField(next, "partitionSpec"));
            assertEquals(partition, config.getString("partitionSpec"));
        }
    }

    @Test
    public void publicPreparationShouldResolveFunctionsAndRejectTunnelSql() throws Exception {
        OdpsReader reader = initializedReader(readerConfig("auto", "", "region='$toLower(\"CN\")'"));
        reader.prepare();
        assertEquals("tunnel", readerField(reader, "readMode"));
        assertEquals("region='cn'", readerField(reader, "partitionSpec"));

        reader = initializedReader(readerConfig("tunnel", "select * from orders", ""));
        try {
            reader.prepare();
            fail("Explicit Tunnel mode must reject selectSql");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("selectSql requires"));
        }
    }

    @Test
    public void buildSelectSqlShouldUseTableColumnsAndPartitions() {
        String sql = OdpsReader.buildSelectSql(
                "orders",
                Arrays.asList("id", "customer_name", "id"),
                "dt='20260605', region=\"gd\"");

        assertEquals("select id, customer_name from orders where dt = '20260605' and region = 'gd';", sql);
    }

    @Test
    public void buildSelectSqlShouldUseWildcardWhenColumnsMissing() {
        assertEquals("select * from orders;", OdpsReader.buildSelectSql("orders", null, null));
        assertEquals("select * from orders;", OdpsReader.buildSelectSql("orders", Arrays.asList("*"), ""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resolveReadModeShouldRejectUnknownMode() {
        OdpsReader.resolveReadMode("jdbc", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void explicitTunnelModeShouldRejectSelectSql() {
        OdpsReader.validateReadModeSqlCompatibility("tunnel", "select * from t");
    }

    @Test
    public void readCountShouldRespectOffsetAndMaxRows() {
        assertEquals(0L, OdpsReader.readCount(0L, 0L, 0L));
        assertEquals(90L, OdpsReader.readCount(100L, 10L, 0L));
        assertEquals(5L, OdpsReader.readCount(100L, 10L, 5L));
        assertEquals(0L, OdpsReader.readCount(100L, 100L, 5L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void readCountShouldRejectNegativeOffset() {
        OdpsReader.readCount(100L, -1L, 0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void readCountShouldRejectNegativeMaxRows() {
        OdpsReader.readCount(100L, 0L, -1L);
    }

    @Test
    public void readWindowShouldAllowFullResultRead() {
        OdpsReader.validateReadWindow(20L, 0L);
    }

    @Test
    public void configurationSetMapValueShouldKeepLiteralDotKey() {
        Configuration configuration = Configuration.newDefault();
        configuration.setMapValue("connect.extraParams", "odps.sql.allow.fullscan", "true");

        assertEquals("true", configuration.getMapValue("connect.extraParams", "odps.sql.allow.fullscan"));
        assertNull(configuration.getString("connect.extraParams.odps.sql.allow.fullscan"));
    }

    @Test
    public void parseDataSourceShouldFlattenNestedExtraParamsFromPathSet() {
        Configuration connectConfig = Configuration.newDefault();
        connectConfig.set("type", "odps");
        connectConfig.set("host", "http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        connectConfig.set("database", "project");
        connectConfig.set("userName", "accessId");
        connectConfig.set("password", "accessKey");
        connectConfig.set("extraParams.odps.sql.allow.fullscan", "true");
        connectConfig.set("extraParams.projectEnv", "dev");

        BaseDataSourceDTO dataSource = OdpsReader.parseDataSource(connectConfig);

        assertEquals("true", dataSource.getExtraParams().get("odps.sql.allow.fullscan"));
        assertEquals("dev", dataSource.getExtraParams().get("projectEnv"));
        assertFalse(dataSource.getExtraParams().containsKey("odps"));
    }

    @Test
    public void parseDataSourceShouldAcceptExtraParamsJsonString() {
        Configuration connectConfig = Configuration.newDefault();
        connectConfig.set("type", "odps");
        connectConfig.set("host", "http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        connectConfig.set("database", "project");
        connectConfig.set("userName", "accessId");
        connectConfig.set("password", "accessKey");
        connectConfig.set("extraParams", "{\"odps.sql.allow.fullscan\":\"true\",\"projectEnv\":\"dev\"}");

        BaseDataSourceDTO dataSource = OdpsReader.parseDataSource(connectConfig);

        assertEquals("true", dataSource.getExtraParams().get("odps.sql.allow.fullscan"));
        assertEquals("dev", dataSource.getExtraParams().get("projectEnv"));
        assertFalse(dataSource.getExtraParams().containsKey("odps"));
    }

    @Test
    public void normalizeExtraParamsShouldPreferTopLevelLiteralKey() {
        Map<String, Object> allow = new LinkedHashMap<>();
        allow.put("fullscan", "true");
        Map<String, Object> sql = new LinkedHashMap<>();
        sql.put("allow", allow);
        Map<String, Object> odps = new LinkedHashMap<>();
        odps.put("sql", sql);
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("odps", odps);
        raw.put("odps.sql.allow.fullscan", "false");

        Map<String, String> normalized = OdpsUtils.normalizeExtraParams(raw);

        assertEquals("false", normalized.get("odps.sql.allow.fullscan"));
    }

    @Test
    public void partitionValuesShouldParseQuotedValues() {
        Map<String, String> values = OdpsReader.partitionValues("pt='20260603', region=\"cn\"");
        assertEquals("20260603", values.get("pt"));
        assertEquals("cn", values.get("region"));
    }

    @Test
    public void addColumnsShouldPreserveConfiguredOrderAndNormalizeValues() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 7);
        row.put("name", "\\N");
        row.put("enabled", true);
        row.put("amount", new BigDecimal("12.34"));

        DefaultRecord record = new DefaultRecord();
        OdpsReader.addColumns(record, Arrays.asList("name", "id", "amount", "enabled"), row);

        assertEquals(4, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof StringColumn);
        assertEquals("", record.getColumn(0).asString());
        assertTrue(record.getColumn(1) instanceof LongColumn);
        assertEquals(Long.valueOf(7L), record.getColumn(1).asLong());
        assertTrue(record.getColumn(2) instanceof DoubleColumn);
        assertEquals(Double.valueOf(12.34D), record.getColumn(2).asDouble());
        assertTrue(record.getColumn(3) instanceof BoolColumn);
        assertEquals(Boolean.TRUE, record.getColumn(3).asBoolean());
    }

    @Test
    public void tunnelColumnsShouldDecodeTextBytesButPreserveBinaryBytes() {
        TableSchema schema = new TableSchema();
        schema.addColumn(new Column("name", OdpsType.STRING));
        schema.addColumn(new Column("payload", OdpsType.BINARY));
        ArrayRecord source = new ArrayRecord(schema);
        source.set("name", "中文".getBytes(StandardCharsets.UTF_8));
        source.setBinary("payload", new Binary(new byte[]{0, 1, 2}));

        DefaultRecord target = new DefaultRecord();
        OdpsReader.addColumns(target, Arrays.asList("payload", "name"), source, java.util.Collections.emptyMap());

        assertEquals(2, target.getColumnNumber());
        assertTrue(target.getColumn(0) instanceof com.jdragon.aggregation.commons.element.BytesColumn);
        assertTrue(target.getColumn(1) instanceof StringColumn);
        assertEquals("中文", target.getColumn(1).asString());
    }

    @Test
    public void resolveProjectNameShouldAppendDevSuffixOnlyForDevEnv() {
        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
        dataSource.setDatabase("project");
        dataSource.getExtraParams().put("projectEnv", "dev");
        assertEquals("project_dev", OdpsReader.resolveProjectName(dataSource));

        dataSource.setDatabase("project_dev");
        assertEquals("project_dev", OdpsReader.resolveProjectName(dataSource));

        dataSource.setDatabase("project");
        dataSource.getExtraParams().put("projectEnv", "prod");
        assertEquals("project", OdpsReader.resolveProjectName(dataSource));
    }

    @Test
    public void normalizeColumnsShouldTrimAndDropBlankColumns() {
        List<String> columns = OdpsReader.normalizeColumns(Arrays.asList(" id ", "", null, "name"));
        assertEquals(Arrays.asList("id", "name"), columns);
    }

    private static Configuration readerConfig(String mode, String sql, String partition) {
        Configuration config = Configuration.newDefault();
        config.set("connect.type", "odps");
        config.set("connect.host", "http://127.0.0.1:1");
        config.set("connect.database", "project");
        config.set("connect.userName", "dummy-access-id");
        config.set("connect.password", "dummy-access-key");
        config.set("table", "orders");
        config.set("readMode", mode);
        config.set("selectSql", sql);
        config.set("partitionSpec", partition);
        return config;
    }

    private static OdpsReader initializedReader(Configuration config) {
        OdpsReader reader = new OdpsReader();
        reader.setPluginJobConf(config);
        reader.init();
        return reader;
    }

    private static Object readerField(OdpsReader reader, String name) throws Exception {
        Field field = OdpsReader.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(reader);
    }
}
