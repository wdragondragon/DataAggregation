package com.jdragon.aggregation.odps;

import com.jdragon.aggregation.commons.element.BoolColumn;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.odps.OdpsUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OdpsReaderTest {

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
}
