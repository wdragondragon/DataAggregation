package com.jdragon.aggregation.datasource.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetUtils {

    public static String getStringSafe(ResultSet rs, String columnLabel) {
        try {
            return rs.getString(columnLabel);
        } catch (SQLException e) {
            return null;
        }
    }

    public static int getIntSafe(ResultSet rs, String columnLabel) {
        try {
            return rs.getInt(columnLabel);
        } catch (SQLException e) {
            return 0;
        }
    }

    public static boolean getBooleanSafe(ResultSet rs, String columnLabel) {
        try {
            return rs.getBoolean(columnLabel);
        } catch (SQLException e) {
            return false;
        }
    }
}
