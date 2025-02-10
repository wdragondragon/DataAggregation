package com.jdragon.aggregation.rdbms.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.RetryUtil;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private static final ThreadLocal<ExecutorService> rsExecutors = new ThreadLocal<ExecutorService>() {
        @Override
        protected ExecutorService initialValue() {
            return Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("rsExecutors-%d")
                    .setDaemon(true)
                    .build());
        }
    };

    private DBUtil() {
    }


    public static Connection getConnection(final RdbmsSourcePlugin rdbmsSourcePlugin, final BaseDataSourceDTO dataSourceDTO) {
        try {
            return RetryUtil.executeWithRetry(
                    () -> rdbmsSourcePlugin.getConnection(dataSourceDTO),
                    3, 1000L, true);
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.CONN_DB_ERROR, "数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", e);
        }
    }


    private static synchronized Connection connect(RdbmsSourcePlugin rdbmsSourcePlugin,
                                                   String url, String user, String pass, String socketTimeout) {

        //ob10的处理
        DataSourceType dataBaseType = rdbmsSourcePlugin.getType();
        if (url.startsWith(com.jdragon.aggregation.rdbms.writer.Constant.OB10_SPLIT_STRING) &&
                (dataBaseType == DataSourceType.MySql || dataBaseType == DataSourceType.Mysql8)) {
            String[] ss = url.split(com.jdragon.aggregation.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw AggregationException
                        .asException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系ask");
            }
            LOG.info("this is ob1_0 jdbc url.");
            user = ss[1].trim() + ":" + user;
            url = ss[2];
            LOG.info("this is ob1_0 jdbc url. user=" + user + " :url=" + url);
        }

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DataSourceType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(rdbmsSourcePlugin, url, prop);
    }

    private static synchronized Connection connect(RdbmsSourcePlugin rdbmsSourcePlugin,
                                                   String url, Properties prop) {
        DataSourceType dataBaseType = rdbmsSourcePlugin.getType();
        try {
            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            // oracle类型报出服务未找到时，需要重试使用sid方式获取连接
            if (dataBaseType != DataSourceType.Oracle
                    || !e.getMessage().contains("TNS:listener does not currently know of service requested in connect descriptor")) {
                throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
            }
        }
        // oracle类型报出服务未找到时，需要重试使用sid方式获取连接
        LOG.warn("使用oracle server name连接失败，尝试使用sid");
        String newJdbcUrl = url.replaceAll("@//", "@").replaceAll("/", ":");
        LOG.info("oracle链接变更{}->{}", url, newJdbcUrl);
        try {
            return DriverManager.getConnection(newJdbcUrl, prop);
        } catch (Exception e) {
            throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn         Database connection .
     * @param sql          sql statement to be executed
     * @param fetchSize
     * @param queryTimeout unit:second
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        try {
            conn.setAutoCommit(false);
        } catch (Exception e) {
            LOG.warn("数据源不支持设置autoCommit");
        }
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        try {
            stmt.setQueryTimeout(queryTimeout);
        } catch (Exception e) {
            LOG.warn("数据源不支持设置queryTimeout");
        }
        return query(stmt, sql);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
                rs.close();
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(RdbmsSourcePlugin rdbmsSourcePlugin, BaseDataSourceDTO dataSourceDTO, String tableName) {
        Connection conn = getConnection(rdbmsSourcePlugin, dataSourceDTO);
        return getTableColumnsByConn(rdbmsSourcePlugin, conn, tableName);
    }

    //经过"或`转义的列名。用来预防有保留关键字字段
    public static List<String> handleKeywords(RdbmsSourcePlugin rdbmsSourcePlugin, List<String> columns) {
        List<String> findColumns = new LinkedList<>();
        DataSourceType dataBaseType = rdbmsSourcePlugin.getType();
        //mysql使用` 其他数据库使用"
        for (String column : columns) {
            if (DataSourceType.MySql == dataBaseType || dataBaseType == DataSourceType.Mysql8 || dataBaseType == DataSourceType.HIVE) {
                findColumns.add("`" + column + "`");
            } else {
                findColumns.add("\"" + column + "\"");
            }
        }
        return findColumns;
    }

    //经过"或`转义的table。用来预防有保留关键字字段
    public static List<String> handleTableKeywords(RdbmsSourcePlugin rdbmsSourcePlugin, List<String> tables) {
        DataSourceType dataBaseType = rdbmsSourcePlugin.getType();
        List<String> findColumns = new LinkedList<>();
        //mysql使用` 其他数据库使用"
        for (String table : tables) {
            String[] split = table.split("\\.");
            if (split.length == 2) {
                if (DataSourceType.MySql == dataBaseType || dataBaseType == DataSourceType.Mysql8 || dataBaseType == DataSourceType.HIVE || dataBaseType == DataSourceType.ClickHouse) {
                    findColumns.add("`" + split[0] + "`" + "." + "`" + split[1] + "`");
                } else {
                    findColumns.add("\"" + split[0] + "\"" + "." + "\"" + split[1] + "\"");
                }
            } else {
                if (DataSourceType.MySql == dataBaseType || dataBaseType == DataSourceType.Mysql8 || dataBaseType == DataSourceType.HIVE || dataBaseType == DataSourceType.ClickHouse) {
                    findColumns.add("`" + table + "`");
                } else {
                    findColumns.add("\"" + table + "\"");
                }
            }
        }
        LOG.info("table handler key {}", JSONObject.toJSONString(findColumns));
        return findColumns;
    }

    public static List<String> getTableColumnsByConn(RdbmsSourcePlugin rdbmsSourcePlugin, Connection conn, String tableName) {
        DataSourceType dataBaseType = rdbmsSourcePlugin.getType();
        List<String> columns = new ArrayList<String>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql = null;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                    tableName);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }

        } catch (SQLException e) {
            throw RdbmsException.asQueryException(dataBaseType, e, queryColumnSql, tableName, null);
        } finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            RdbmsSourcePlugin rdbmsSourcePlugin, BaseDataSourceDTO dataSourceDTO, String tableName, String column) {
        Connection conn = null;
        try {
            conn = getConnection(rdbmsSourcePlugin, dataSourceDTO);
            return getColumnMetaData(conn, tableName, column);
        } finally {
            DBUtil.closeDBResources(null, null, conn);
        }
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<>(), new ArrayList<>(),
                new ArrayList<>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";
            LOG.info(String.format("Executing sql: [%s]", queryColumnSql));
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;

        } catch (SQLException e) {
            throw AggregationException
                    .asException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                            String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }


    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        //默认3600 seconds
        stmt.setQueryTimeout(Constant.SOCKET_TIMEOUT_INSECOND);
        return query(stmt, sql);
    }

    public static void loadDriverClass(String pluginType, String pluginName) {
        try {
            String pluginJsonPath = StringUtils.join(
                    new String[]{System.getProperty("datax.home"), "plugin",
                            pluginType,
                            String.format("%s%s", pluginName, pluginType),
                            "plugin.json"}, File.separator);
            Configuration configuration = Configuration.from(new File(
                    pluginJsonPath));
            List<String> drivers = configuration.getList("drivers",
                    String.class);
            for (String driver : drivers) {
                Class.forName(driver);
            }
        } catch (ClassNotFoundException e) {
            throw AggregationException.asException(DBUtilErrorCode.CONF_ERROR,
                    "数据库驱动加载错误, 请确认libs目录有驱动jar包且plugin.json中drivers配置驱动类正确!",
                    e);
        }
    }
}
