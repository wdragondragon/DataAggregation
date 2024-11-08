package com.jdragon.aggregation.core;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
        // 获取当前时间的毫秒值
        long currentTimeMillis = System.currentTimeMillis();

        // 创建 SimpleDateFormat 对象，设置格式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmmss-SSS");

        // 使用当前时间格式化
        String formattedDate = dateFormat.format(new Date(currentTimeMillis));

        // 打印结果
        System.out.println(formattedDate);
    }
}
