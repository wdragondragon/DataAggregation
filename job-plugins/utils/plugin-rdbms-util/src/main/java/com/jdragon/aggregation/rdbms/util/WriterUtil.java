package com.jdragon.aggregation.rdbms.util;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class WriterUtil {

    protected static final Logger LOG = LoggerFactory
            .getLogger(WriterUtil.class);

    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode, RdbmsSourcePlugin sourcePlugin, boolean forceUseUpdate, List<String> pks) {
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("replace")
                || writeMode.trim().toLowerCase().startsWith("update")
                || writeMode.trim().toLowerCase().startsWith("copy");

        if (!isWriteModeLegal) {
            throw AggregationException.asException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update,copy 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        DataSourceType dataBaseType = sourcePlugin.getType();
        //经过"或`转义的列名。用来预防有保留关键字字段
        List<String> findColumns = DBUtil.handleKeywords(sourcePlugin, columnHolders);
        List<String> handlePks = DBUtil.handleKeywords(sourcePlugin, pks);
        // && writeMode.trim().toLowerCase().startsWith("replace")
        String writeDataSqlTemplate;
        if (forceUseUpdate ||
                ((dataBaseType == DataSourceType.MySql || dataBaseType == DataSourceType.Mysql8) && writeMode.trim().toLowerCase().startsWith("update"))
        ) {
            //update只在mysql下使用
            writeDataSqlTemplate = "INSERT INTO %s (" + StringUtils.join(findColumns, ",") +
                    ") VALUES(" + StringUtils.join(valueHolders, ",") +
                    ")" +
                    onDuplicateKeyUpdateString(findColumns);
        } else if (dataBaseType == DataSourceType.PostgreSQL && writeMode.trim().toLowerCase().startsWith("update")) {
            writeDataSqlTemplate = "INSERT INTO %s (" +
                    StringUtils.join(findColumns, ",") +
                    ") VALUES(" + StringUtils.join(valueHolders, ",") +
                    ")" + onConFlictDoString(writeMode, findColumns, handlePks);
        } else if ((dataBaseType == DataSourceType.PostgreSQL) && writeMode.trim().toLowerCase().startsWith("copy")) {
            writeDataSqlTemplate = "COPY %s (" + StringUtils.join(findColumns, ",") + ") FROM STDIN";
        } else if (!pks.isEmpty() && dataBaseType == DataSourceType.DM && writeMode.trim().toLowerCase().startsWith("replace")) {
            List<String> columnList = columnHolders.stream().filter(s -> !pks.contains(s)).collect(Collectors.toList());

            String insert = "insert (" + join(columnHolders, "%s", ",") + ")" +
                    " values(" + join(columnHolders, "t.%s", ",") + ")";

            String update = "update set " + join(columnList, "a.%s=t.%s", ",");

            String select = "select " + join(columnHolders, "? %s", ",") + " from dual";

            writeDataSqlTemplate = "merge into %s a" +
                    " using (" + select + ") t " +
                    " on(" + join(pks, "a.%s=t.%s", " and ") + ")" +
                    " when matched then " +
                    update +
                    " when not matched then " +
                    insert;
            LOG.info("识别到是DM的replace采集，改变采集语句[{}]", writeDataSqlTemplate);
        } else {
            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (writeMode.trim().toLowerCase().startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate = writeMode +
                    " INTO %s (" + StringUtils.join(findColumns, ",") +
                    ") VALUES(" + StringUtils.join(valueHolders, ",") +
                    ")";
        }
        return writeDataSqlTemplate;
    }

    private static String onConFlictDoString(String conflict, List<String> columnHolders, List<String> pks) {
        StringBuilder sb = new StringBuilder();
        sb.append(" ON CONFLICT (");
        sb.append(StringUtils.join(pks, ","));
        sb.append(") DO ");
        if (columnHolders == null || columnHolders.size() < 1) {
            sb.append("NOTHING");
            return sb.toString();
        }
        sb.append(" UPDATE SET ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(column);
            sb.append("=excluded.");
            sb.append(column);
        }
        return sb.toString();
    }

    public static String join(List<String> strings, String template, String separator) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < strings.size(); i++) {
            String s = strings.get(i);
            stringBuffer.append(template.replaceAll("%s", s));
            if (i < strings.size() - 1) {
                stringBuffer.append(separator);
            }
        }
        return stringBuffer.toString();
    }

    public static String onDuplicateKeyUpdateString(List<String> columnHolders) {
        if (columnHolders == null || columnHolders.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }

        return sb.toString();
    }

}
