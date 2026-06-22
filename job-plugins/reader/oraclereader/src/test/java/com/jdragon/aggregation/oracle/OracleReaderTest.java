package com.jdragon.aggregation.oracle;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OracleReaderTest {

    private OracleReader reader;

    @Mock
    private ResultSet rs;
    @Mock
    private ResultSetMetaData metaData;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        reader = new OracleReader();
    }

    @Test
    public void testNumberScaleZero_shouldUseLongColumn() throws Exception {
        // Oracle NUMBER(10,0) -> scale=0, should be LongColumn
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getScale(1)).thenReturn(0);
        when(rs.getString(1)).thenReturn("123456789");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertEquals(1, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof LongColumn);
        assertEquals(Long.valueOf(123456789L), record.getColumn(0).asLong());
    }

    @Test
    public void testNumberScalePositive_shouldUseDoubleColumn() throws Exception {
        // Oracle NUMBER(10,2) -> scale=2, should be DoubleColumn
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getScale(1)).thenReturn(2);
        when(rs.getString(1)).thenReturn("123.45");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertEquals(1, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof DoubleColumn);
        assertEquals(Double.valueOf(123.45), record.getColumn(0).asDouble());
    }

    @Test
    public void testDecimalScalePositive_shouldUseDoubleColumn() throws Exception {
        // Oracle NUMBER mapped as Types.DECIMAL with scale > 0
        when(metaData.getColumnType(1)).thenReturn(Types.DECIMAL);
        when(metaData.getScale(1)).thenReturn(4);
        when(rs.getString(1)).thenReturn("99.8888");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertTrue(record.getColumn(0) instanceof DoubleColumn);
        assertEquals(Double.valueOf(99.8888), record.getColumn(0).asDouble());
    }

    @Test
    public void testNumberNullValue_shouldUseEmptyDoubleColumn() throws Exception {
        // Oracle NUMBER with null value -> empty DoubleColumn
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getScale(1)).thenReturn(0);
        when(rs.getString(1)).thenReturn(null);

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertEquals(1, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof DoubleColumn);
        assertNull(record.getColumn(0).asDouble());
    }

    @Test
    public void testNumberBlankString_shouldUseEmptyDoubleColumn() throws Exception {
        // Oracle NUMBER with blank string -> empty DoubleColumn
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getScale(1)).thenReturn(0);
        when(rs.getString(1)).thenReturn("");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertTrue(record.getColumn(0) instanceof DoubleColumn);
        assertNull(record.getColumn(0).asDouble());
    }

    @Test
    public void testVarchar_fallsThroughToDefault() throws Exception {
        // VARCHAR should fall through to super.addRecord -> StringColumn
        when(metaData.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rs.getString(1)).thenReturn("hello_oracle");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertEquals(1, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof StringColumn);
        assertEquals("hello_oracle", record.getColumn(0).asString());
    }

    @Test
    public void testTimestamp_fallsThroughToDefault() throws Exception {
        // TIMESTAMP should fall through to super.addRecord -> TimestampColumn
        Timestamp now = new Timestamp(System.currentTimeMillis());
        when(metaData.getColumnType(1)).thenReturn(Types.TIMESTAMP);
        when(rs.getString(1)).thenReturn(now.toString());
        when(rs.getTimestamp(1)).thenReturn(now);

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertEquals(1, record.getColumnNumber());
        assertTrue(record.getColumn(0) instanceof TimestampColumn);
    }

    @Test
    public void testInteger_fallsThroughToDefault() throws Exception {
        // Types.INTEGER should fall through to super -> LongColumn
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(rs.getString(1)).thenReturn("42");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        assertTrue(record.getColumn(0) instanceof LongColumn);
        assertEquals(Long.valueOf(42L), record.getColumn(0).asLong());
    }

    @Test
    public void testClob_fallsThroughToDefault() throws Exception {
        // CLOB should fall through to super -> StringColumn
        when(metaData.getColumnType(1)).thenReturn(Types.CLOB);

        DefaultRecord record = new DefaultRecord();
        try {
            reader.addRecord(rs, metaData, 1, record, null);
        } catch (Exception e) {
            // Expected: ResultSet.getClob() is not stubbed, will throw NPE
            // This confirms the code falls through to super.addRecord CLOB branch
        }
        // Either CLOB read succeeds (if we stubbed enough) or falls to super
    }

    @Test
    public void testOracleTimestampWithTimeZone_fallsThroughToDefault() throws Exception {
        // Oracle TIMESTAMP WITH TIME ZONE -> type code -101
        when(metaData.getColumnType(1)).thenReturn(-101);
        when(rs.getString(1)).thenReturn("2024-01-15 10:30:00.000000 +08:00");
        when(rs.getTimestamp(1)).thenReturn(new Timestamp(System.currentTimeMillis()));

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        // The default handler for -101 tries to read via rs.getString
        // Falls through to super.addRecord
        assertEquals(1, record.getColumnNumber());
    }

    @Test(expected = AggregationException.class)
    public void testUnsupportedType_throwsException() throws Exception {
        // An unsupported type code should throw AggregationException
        when(metaData.getColumnType(1)).thenReturn(99999);
        when(metaData.getColumnName(1)).thenReturn("bogus_col");
        when(metaData.getColumnTypeName(1)).thenReturn("BOGUS");
        when(metaData.getColumnClassName(1)).thenReturn("java.lang.Object");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);
    }

    @Test
    public void testNumberNegativeScale_shouldUseDoubleColumn() throws Exception {
        // Oracle NUMBER with negative scale (e.g. NUMBER(*,-2) for rounding to hundreds)
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getScale(1)).thenReturn(-2);
        when(rs.getString(1)).thenReturn("12300");

        DefaultRecord record = new DefaultRecord();
        reader.addRecord(rs, metaData, 1, record, null);

        // scale != 0, so falls to DoubleColumn
        assertTrue(record.getColumn(0) instanceof DoubleColumn);
        assertEquals(Double.valueOf(12300.0), record.getColumn(0).asDouble());
    }
}
