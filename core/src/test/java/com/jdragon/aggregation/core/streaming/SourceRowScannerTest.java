package com.jdragon.aggregation.core.streaming;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import com.jdragon.aggregation.datasource.TableInfo;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SourceRowScannerTest {

    @Test
    public void shouldUseScanQueryForDatabaseSourcesAndKeepMappingAndMaxRecords() {
        FakeDataSourcePlugin plugin = new FakeDataSourcePlugin();
        SourceRowScanner scanner = new SourceRowScanner(new FakePluginManager(plugin));
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId("sourceA");
        config.setSourceName("Source A");
        config.setPluginName("mysql8");
        config.setQuerySql("select src, ignored from t");
        config.setMaxRecords(2);
        Map<String, String> mappings = new LinkedHashMap<String, String>();
        mappings.put("src", "dst");
        config.setFieldMappings(mappings);

        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        scanner.scan(config, rows::add);

        assertTrue(plugin.scanQueryCalled);
        assertFalse(plugin.executeQuerySqlCalled);
        assertEquals("select src, ignored from t LIMIT 2", plugin.lastSql);
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).get("dst"));
        assertEquals("b", rows.get(1).get("dst"));
        assertFalse(rows.get(0).containsKey("src"));
    }

    private static class FakePluginManager extends DataSourcePluginManager {
        private final AbstractDataSourcePlugin plugin;

        FakePluginManager(AbstractDataSourcePlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public AbstractDataSourcePlugin getDataSourcePlugin(String pluginName) {
            return plugin;
        }
    }

    private static class FakeDataSourcePlugin extends AbstractDataSourcePlugin {
        private boolean executeQuerySqlCalled;
        private boolean scanQueryCalled;
        private String lastSql;

        @Override
        public Connection getConnection(BaseDataSourceDTO dataSource) {
            return null;
        }

        @Override
        public Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
            executeQuerySqlCalled = true;
            throw new AssertionError("executeQuerySql should not be used by SourceRowScanner database path");
        }

        @Override
        public void scanQuery(BaseDataSourceDTO dataSource, String sql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer) {
            scanQueryCalled = true;
            lastSql = sql;
            for (String value : Arrays.asList("a", "b", "c")) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                row.put("src", value);
                row.put("ignored", "ignored-" + value);
                rowConsumer.accept(row);
            }
        }

        @Override
        public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        }

        @Override
        public void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList) {
        }

        @Override
        public Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
            return null;
        }

        @Override
        public void insertData(InsertDataDTO insertDataDTO) {
        }

        @Override
        public List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table) {
            return Collections.emptyList();
        }

        @Override
        public List<String> getTableNames(BaseDataSourceDTO dataSource, String table) {
            return Collections.emptyList();
        }

        @Override
        public List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table) {
            return Collections.emptyList();
        }

        @Override
        public String getTableSize(BaseDataSourceDTO dataSource, String table) {
            return "0";
        }

        @Override
        public Long getTableCount(BaseDataSourceDTO dataSource, String table) {
            return 0L;
        }
    }
}
