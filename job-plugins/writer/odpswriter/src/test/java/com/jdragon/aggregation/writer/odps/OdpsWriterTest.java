package com.jdragon.aggregation.writer.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.jdragon.aggregation.commons.element.BoolColumn;
import com.jdragon.aggregation.commons.element.BytesColumn;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OdpsWriterTest {

    @Test
    public void resolveWriteModeShouldAllowAppendAndOverwrite() {
        assertEquals(OdpsWriter.WRITE_MODE_APPEND, OdpsWriter.resolveWriteMode(null));
        assertEquals(OdpsWriter.WRITE_MODE_APPEND, OdpsWriter.resolveWriteMode("APPEND"));
        assertEquals(OdpsWriter.WRITE_MODE_OVERWRITE, OdpsWriter.resolveWriteMode(" overwrite "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resolveWriteModeShouldRejectUnsupportedMode() {
        OdpsWriter.resolveWriteMode("upsert");
    }

    @Test
    public void normalizeColumnsShouldTrimDropBlankAndDeduplicate() {
        assertEquals(Arrays.asList("id", "name"), OdpsWriter.normalizeColumns(Arrays.asList(" id ", "", null, "name", "id")));
    }

    @Test
    public void wildcardColumnsShouldResolveToDataColumnsOnly() {
        OdpsWriter.WritePlan plan = OdpsWriter.buildWritePlan(Arrays.asList("*"), null, null, nonPartitionSchema());

        assertEquals(4, plan.dataBindings.size());
        assertEquals("id", plan.dataBindings.get(0).columnName);
        assertEquals("customer_name", plan.dataBindings.get(1).columnName);
        assertEquals("amount", plan.dataBindings.get(2).columnName);
        assertEquals("biz_time", plan.dataBindings.get(3).columnName);
    }

    @Test
    public void staticPartitionShouldExcludePartitionColumnFromDataBindings() {
        OdpsWriter.WritePlan plan = OdpsWriter.buildWritePlan(
                Arrays.asList("id", "customer_name", "dt"),
                null,
                "dt='20260605'",
                partitionSchema());

        assertTrue(plan.partitioned);
        assertEquals(2, plan.dataBindings.size());
        assertEquals("id", plan.dataBindings.get(0).columnName);
        assertEquals("customer_name", plan.dataBindings.get(1).columnName);
        assertEquals("dt='20260605'", plan.staticPartitionSpec.toString());
    }

    @Test
    public void dynamicPartitionShouldBuildPartitionBindingAndDataBindings() {
        OdpsWriter.WritePlan plan = OdpsWriter.buildWritePlan(
                Arrays.asList("id", "customer_name", "dt"),
                Arrays.asList("dt"),
                null,
                partitionSchema());

        assertEquals(2, plan.dataBindings.size());
        assertEquals(1, plan.partitionBindings.size());
        assertEquals("dt", plan.partitionBindings.get(0).columnName);
        assertEquals(2, plan.partitionBindings.get(0).index);
    }

    @Test
    public void dynamicPartitionOverwriteShouldRejectMultipleUploadContexts() {
        assertTrue(OdpsWriter.shouldRejectDynamicPartitionOverwrite(
                OdpsWriter.WRITE_MODE_OVERWRITE, true, 2));
    }

    @Test
    public void dynamicPartitionOverwriteShouldAllowSingleUploadContext() {
        assertEquals(false, OdpsWriter.shouldRejectDynamicPartitionOverwrite(
                OdpsWriter.WRITE_MODE_OVERWRITE, true, 1));
    }

    @Test
    public void dynamicPartitionAppendShouldAllowMultipleUploadContexts() {
        assertEquals(false, OdpsWriter.shouldRejectDynamicPartitionOverwrite(
                OdpsWriter.WRITE_MODE_APPEND, true, 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void partitionSpecAndPartitionColumnsShouldConflict() {
        OdpsWriter.buildWritePlan(
                Arrays.asList("id", "dt"),
                Arrays.asList("dt"),
                "dt='20260605'",
                partitionSchema());
    }

    @Test(expected = IllegalArgumentException.class)
    public void partitionedTableShouldRequirePartitionSource() {
        OdpsWriter.buildWritePlan(Arrays.asList("id"), null, null, partitionSchema());
    }

    @Test(expected = IllegalArgumentException.class)
    public void dynamicPartitionShouldRejectWildcardColumns() {
        OdpsWriter.buildWritePlan(Arrays.asList("*"), Arrays.asList("dt"), null, partitionSchema());
    }

    @Test
    public void partitionValuesShouldParseQuotedValues() {
        Map<String, String> values = OdpsWriter.partitionValues("dt='20260605', region=\"gd\"");
        assertEquals("20260605", values.get("dt"));
        assertEquals("gd", values.get("region"));
    }

    @Test
    public void canonicalPartitionSpecShouldFollowSchemaOrder() {
        assertEquals("dt='20260605',region='gd'",
                OdpsWriter.canonicalPartitionSpec(
                        OdpsWriter.partitionValues("region='gd',dt='20260605'"),
                        Arrays.asList("dt", "region")));
    }

    @Test
    public void toOdpsValueShouldConvertPrimitiveTypes() {
        assertEquals(Long.valueOf(7L), OdpsWriter.toOdpsValue(new LongColumn(7L), new Column("id", OdpsType.BIGINT), false));
        assertEquals(Integer.valueOf(7), OdpsWriter.toOdpsValue(new LongColumn(7L), new Column("id", OdpsType.INT), false));
        assertEquals(Short.valueOf((short) 7), OdpsWriter.toOdpsValue(new LongColumn(7L), new Column("id", OdpsType.SMALLINT), false));
        assertEquals(Byte.valueOf((byte) 7), OdpsWriter.toOdpsValue(new LongColumn(7L), new Column("id", OdpsType.TINYINT), false));
        assertEquals(Double.valueOf(12.3D), OdpsWriter.toOdpsValue(new DoubleColumn(12.3D), new Column("amount", OdpsType.DOUBLE), false));
        assertEquals(Float.valueOf(12.3F), OdpsWriter.toOdpsValue(new DoubleColumn(12.3D), new Column("amount", OdpsType.FLOAT), false));
        assertEquals(new BigDecimal("12.30"), OdpsWriter.toOdpsValue(new StringColumn("12.30"), new Column("amount", OdpsType.DECIMAL), false));
        assertEquals(Boolean.TRUE, OdpsWriter.toOdpsValue(new BoolColumn(true), new Column("enabled", OdpsType.BOOLEAN), false));
        assertEquals("abc", OdpsWriter.toOdpsValue(new StringColumn("abc"), new Column("name", OdpsType.STRING), false));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) OdpsWriter.toOdpsValue(new BytesColumn(new byte[]{1, 2}), new Column("bin", OdpsType.BINARY), false));
    }

    @Test
    public void toOdpsValueShouldConvertDates() {
        Timestamp timestamp = Timestamp.valueOf("2026-06-05 10:11:12");
        DateColumn dateColumn = new DateColumn(timestamp);

        assertEquals(timestamp.getTime(), ((java.util.Date) OdpsWriter.toOdpsValue(dateColumn, new Column("d1", OdpsType.DATETIME), false)).getTime());
        assertEquals(new java.sql.Date(timestamp.getTime()), OdpsWriter.toOdpsValue(dateColumn, new Column("d2", OdpsType.DATE), false));
        assertEquals(timestamp, OdpsWriter.toOdpsValue(dateColumn, new Column("d3", OdpsType.TIMESTAMP), false));
    }

    @Test
    public void toOdpsValueShouldSupportEmptyAsNull() {
        assertNull(OdpsWriter.toOdpsValue(new StringColumn(""), new Column("name", OdpsType.STRING), true));
        assertEquals("", OdpsWriter.toOdpsValue(new StringColumn(""), new Column("name", OdpsType.STRING), false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void toOdpsValueShouldRejectComplexTypes() {
        OdpsWriter.toOdpsValue(new StringColumn("{\"a\":1}"), new Column("payload", OdpsType.JSON), false);
    }

    @Test
    public void parseDataSourceShouldFlattenNestedExtraParams() {
        Configuration connectConfig = Configuration.newDefault();
        connectConfig.set("type", "odps");
        connectConfig.set("host", "http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        connectConfig.set("database", "project");
        connectConfig.set("userName", "accessId");
        connectConfig.set("password", "accessKey");
        connectConfig.set("extraParams.odps.sql.allow.fullscan", "true");
        connectConfig.set("extraParams.projectEnv", "dev");

        BaseDataSourceDTO dataSource = OdpsWriter.parseDataSource(connectConfig);

        assertEquals("true", dataSource.getExtraParams().get("odps.sql.allow.fullscan"));
        assertEquals("dev", dataSource.getExtraParams().get("projectEnv"));
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

        BaseDataSourceDTO dataSource = OdpsWriter.parseDataSource(connectConfig);

        assertEquals("true", dataSource.getExtraParams().get("odps.sql.allow.fullscan"));
        assertEquals("dev", dataSource.getExtraParams().get("projectEnv"));
    }

    @Test
    public void resolveProjectNameShouldAppendDevSuffixOnlyForDevEnv() {
        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
        dataSource.setDatabase("project");
        dataSource.getExtraParams().put("projectEnv", "dev");
        assertEquals("project_dev", OdpsWriter.resolveProjectName(dataSource));

        dataSource.setDatabase("project_dev");
        assertEquals("project_dev", OdpsWriter.resolveProjectName(dataSource));

        dataSource.setDatabase("project");
        dataSource.getExtraParams().put("projectEnv", "prod");
        assertEquals("project", OdpsWriter.resolveProjectName(dataSource));
    }

    private static TableSchema nonPartitionSchema() {
        TableSchema schema = new TableSchema();
        schema.addColumn(new Column("id", OdpsType.BIGINT));
        schema.addColumn(new Column("customer_name", OdpsType.STRING));
        schema.addColumn(new Column("amount", OdpsType.DECIMAL));
        schema.addColumn(new Column("biz_time", OdpsType.TIMESTAMP));
        return schema;
    }

    private static TableSchema partitionSchema() {
        TableSchema schema = nonPartitionSchema();
        schema.addPartitionColumn(new Column("dt", OdpsType.STRING));
        return schema;
    }
}
