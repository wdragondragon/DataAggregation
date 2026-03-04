package com.jdragon.aggregation.datasource.file.utils;


import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.datasource.file.utils.efile.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

@Slf4j
public class EFileUtil {

    public EFile parseEFile(File file, String dataType, List<String> dataTag) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(file);
        return parseEFile(fileInputStream, dataType, dataTag);
    }

    public EFile parseEFile(InputStream inputStream, String dataType, List<String> dataTag) throws Exception {
        if (dataTag == null) {
            dataTag = new ArrayList<>();
        }
        EFileInfo eFileInfo = new EFileInfo();
        eFileInfo.setFileType(dataType);

        EFile eFile = new EFile();
        eFile.setEFileInfo(eFileInfo);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            while (true) {
                String line = reader.readLine();
                if (line != null) {
                    line = line.replaceAll("\t", " ");
                    line = line.trim();
                    lineHandler(line, eFile, dataTag);
                } else {
                    break;
                }

            }
        }
        return eFile;
    }

    /**
     * 读取每行数据回调处理方法
     */
    public void lineHandler(String line, EFile eFile, List<String> dataTag) throws Exception {
        EFileInfo eFileInfo;
        List<EFile.EFileDetail> list = eFile.getEFileDetails();
        line = line.replaceAll("	", " ");
        if (line.startsWith("<!") || line.contains("<!")) {
            line = line.substring(line.indexOf("<!"));
            // 获取entity信息
            log.info("扫描到文件tag：{}", line);
            eFileInfo = getEfileInfo(line);
            eFile.setEFileInfo(eFileInfo);
        } else if (line.startsWith("</") && line.endsWith(">")) {
            // 标签结束标识
            log.info("扫描到实体结束：{}", line);
        } else if (line.startsWith("//")) {
            // 无用数据 未做处理
            log.info("扫描到注释数据：{}", line);
        } else if (line.startsWith("@")) {
            getColumn(eFile, line, dataTag);
        } else if (line.indexOf("<") == 0 && line.indexOf(">") == line.length() - 1) {
            // 获取表名
            log.info("扫描到实体tag：{}", line);
            getTableName(list, line);
        } else if (line.startsWith("#")) {
            getData(eFile, line, dataTag);
        } else {
            // 不做处理
            log.info("无法识别内容：{}", line);
        }
    }

    public String errorProcess(String line) {
        return line.replaceAll("@ @", "@");
    }

    /**
     * 获取Entity信息 File tag
     *
     */
    public EFileInfo getEfileInfo(String line) {
        Map<String, String> maps = new HashMap<>();
        if (line != null && line.length() > 2) {

            line = line.substring(2);
            line = line.substring(0, line.length() - 2);

            int end = line.indexOf(" ");
            while (!line.trim().isEmpty() && end >= 0) {

                String map = line.substring(0, end);
                map = map.trim();

                if (!map.isEmpty()) {
                    String[] datas = map.split("=");
                    if (datas.length > 1) {
                        maps.put(datas[0], datas[1]);
                    }
                }
                line = line.substring(end + 1);
                end = line.indexOf(" ");

                if (line.indexOf("'") <= end) {
                    end = line.indexOf("'", line.indexOf("'") + 1) + 1;
                }

            }

        }

        log.info("efileInfo: {}", maps);
        EFileInfo eFileInfo = JSONObject.parseObject(JSONObject.toJSONString(maps), EFileInfo.class);
        eFileInfo.setTags(maps);
        return eFileInfo;
    }

    /**
     * 获取表名 Entity tag
     */
    public void getTableName(List<EFile.EFileDetail> list, String line) {
        Map<String, Object> eFileTableInfoMap = new HashMap<>();
        Map<String, String> tags = new HashMap<>();
        String[] lineSplit = line.substring(1, line.length() - 1).split(" ");
        for (int i = 0; i < lineSplit.length; i++) {
            String item = lineSplit[i];
            int ch = item.indexOf("=");
            if (ch <= 0) {
                if (item.contains("::")) {
                    String[] datas;
                    if (!item.endsWith("::")) {
                        datas = item.split("::");
                    } else {
                        datas = (item + lineSplit[i + 1]).split("::");
                        i++;
                    }
                    eFileTableInfoMap.put("tableName", datas[0]);
                    eFileTableInfoMap.put("tableCode", datas[1]);
                } else {
                    StringBuilder data = new StringBuilder((item + lineSplit[i + 1]));
                    i++;
                    ch = data.indexOf("=");
                    while (ch <= 0 || data.toString().endsWith("=")) {
                        data.append(lineSplit[i + 1]);
                        i++;
                        ch = data.indexOf("=");
                    }
                    String[] datas = data.toString().split("=");
                    tags.put(datas[0], datas[1]);
                }
            } else {
                String[] datas = item.split("=");
                tags.put(datas[0], datas[1]);
            }
        }
        eFileTableInfoMap.putAll(tags);
        EFileTableInfo eFileTableInfo = JSONObject.parseObject(JSONObject.toJSONString(eFileTableInfoMap), EFileTableInfo.class);

        if (eFileTableInfo.getPlanDate() != null) {
            eFileTableInfo.setPlanDate(eFileTableInfo.getPlanDate().replaceAll("'", ""));
        }
        EFile.EFileDetail eFileDetail = new EFile.EFileDetail();
        eFileDetail.setEFileTableInfo(eFileTableInfo);
        eFileDetail.setTags(tags);
        list.add(eFileDetail);
        log.info("表信息：{}", eFileDetail);
    }

    /**
     * 获取字段
     */
    public void getColumn(EFile eFile, String line, List<String> dataTag) {
        List<EFile.EFileDetail> list = eFile.getEFileDetails();
        String str = line.substring(1).trim();
        String[] arr = str.split("[ \t]");
        List<String> slist = new ArrayList<>();
        // 去掉空的值
        for (String colum : arr) {
            if (!colum.isEmpty()) {
                dataTag.remove(colum);
                slist.add(colum);
            }
        }
        slist.addAll(dataTag);
        if (list.isEmpty()) {
            return;
        }

        EFile.EFileDetail map = list.get(list.size() - 1);
        map.setColumn(slist);
        log.info("字段：{}", slist);

    }


    /**
     * 获取字段
     *
     */
    public List<Map<String, Object>> getfields(List<Map<String, Object>> list, String line) {
        String str = line.substring(1).trim();
        String[] arr = str.split("[ \t]");
        List<String> slist = new ArrayList<>();
        // 去掉空的值
        for (String colum : arr) {
            if (!colum.isEmpty()) {
                slist.add(colum);
            }
        }
        Map<String, Object> map = new HashMap<>();
        map.put("column", slist);
        log.info("字段：{}", slist);
        list.add(map);
        return list;
    }


    /**
     * 获取字段备注
     *
     */
    public List<Map<String, Object>> getColumnComments(List<Map<String, Object>> list, String line) {
        String str = line.substring(1).trim();
        String[] arr = str.split("[ \t]");
        List<String> slist = new ArrayList<>();
        // 去掉空的值
        for (int i = 0; i < arr.length - 1; i++) {
            String colum = arr[i];
            if (!colum.isEmpty()) {
                slist.add(colum);
            }
        }
        Map<String, Object> map = new HashMap<>();
        map.put("columncomments", slist);
        log.info("字段备注：{}", slist);
        list.add(map);
        return list;
    }

    //获取字段及字段备注
    public void lineFiledComments(String line, List<Map<String, Object>> list) throws Exception {
        line = line.replaceAll("	", " ");
        if (line.startsWith("//")) {
            // 无用数据 未做处理
            line = line.substring(1);
            getColumnComments(list, line);
        } else if (line.startsWith("@")) {
            getfields(list, line);
        }
    }


    /**
     * 获取数据
     */
    public void getData(EFile eFile, String line, List<String> dataTag) {
        List<EFile.EFileDetail> list = eFile.getEFileDetails();
        EFile.EFileDetail eFileDetail = list.get(list.size() - 1);
        Map<String, String> fileTags = eFile.getEFileInfo().getTags();
        Map<String, String> entityTags = eFileDetail.getTags();

        String str = line.substring(1).trim();
        List<String> dataList = new ArrayList<>();

        String[] dataArr = splitData(str);
        for (String data : dataArr) {
            dataList.add(data.trim());
        }

        for (String tag : dataTag) {
            String tagValue = entityTags.getOrDefault(tag, fileTags.get(tag));
            dataList.add(tagValue);
        }

        List<List<String>> dat = eFileDetail.getData();
        if (dat != null) {
            dat.add(dataList);
        } else {
            List<List<String>> datas = new ArrayList<>();
            datas.add(dataList);
            eFileDetail.setData(datas);
        }
    }


    public String reBuildLine(String line) {
        // 把多个空格合并为一个
        while (line.contains("  ")) {
            line = line.replaceAll(" {2}", " ");
        }

        String sign = "\"";
        if (line.contains("'") && !line.contains("\"")) {
            line = line.replaceAll("'", "\"");
        }
        if (line.contains("“")) {
            line = line.replaceAll("'", "\"");
        }
        if (line.contains("‘")) {
            line = line.replaceAll("'", "\"");
        }

        List<Integer> list = new ArrayList<Integer>();
        if (line.contains(sign)) {
            int i = 0;
            for (char c : line.toCharArray()) {

                if ("\"".equals(String.valueOf(c))) {
                    list.add(i);
                }
                i++;
            }

            if (!list.isEmpty() && list.size() % 2 == 0) {
                for (int j = 0; j < list.size(); j++) {
                    if (j % 2 == 0) {
                        Integer start = list.get(j);
                        start++;
                        Integer end = list.get(j + 1);
                        String tmp = line.substring(start, end);
                        line = line.replaceAll(line.substring(start, end), tmp.replaceAll(" ", ""));
                    }
                }
            }
            line = line.replaceAll(sign, "");
        }
        return line;
    }


    public String[] splitData(String line) {
        List<String> datas = new ArrayList<>();
        // 把多个空格合并为一个
        while (line.contains("  ")) {
            line = line.replaceAll(" {2}", " ");
        }

        String sign = "\"";
        if (line.contains("'") && !line.contains("\"")) {
            line = line.replaceAll("'", "\"");
        }
        if (line.contains("“")) {
            line = line.replaceAll("'", "\"");
        }
        if (line.contains("‘")) {
            line = line.replaceAll("'", "\"");
        }

        List<Integer> list = new ArrayList<Integer>();
        if (line.contains(sign)) {
            int i = 0;
            for (char c : line.toCharArray()) {

                if ("\"".equals(String.valueOf(c))) {
                    list.add(i);
                }
                i++;
            }

            if (!list.isEmpty() && list.size() % 2 == 0) {
                for (int j = 0; j < list.size(); j++) {
                    if (j % 2 == 0) {
                        Integer start = list.get(j);
                        if (j == 0 && start > 0) {
                            String tmp = line.substring(0, start);
                            datas.add(tmp);
                        }
                        start++;
                        Integer end = list.get(j + 1);
                        String tmp = line.substring(start, end);
                        datas.add(tmp);
                    }
                    if (j == list.size() - 1) {
                        Integer end = list.get(j);
                        String tmp = line.substring(end + 1);
                        String[] strs = tmp.trim().split(" ");
                        datas.addAll(Arrays.asList(strs));
                    }
                }
            }
            line = line.replaceAll(sign, "");
        } else {
            return line.trim().split(" ");
        }
        String[] strs = new String[datas.size()];
        return datas.toArray(strs);
    }


    public static File genEFile(String fileName, EFileObject eFileObject) throws Exception {
        String filePath = File.separator + fileName;

        if (!createFile(filePath)) {
            throw new RuntimeException("创建文件失败：" + filePath);
        }
        File file = new File(filePath);

        OutputStream os = new FileOutputStream(file, true);
        OutputStreamWriter writer = new OutputStreamWriter(os);
        BufferedWriter bw = new BufferedWriter(writer);
        // 生成第一行，头信息
        String header = genHeader(eFileObject);
        bufferedWriteByLine(bw, header);

        // 分标签生成数据
        List<EFileTag> tags = eFileObject.getDatas();
        for (EFileTag eFileTag : tags) {
            // 生成标签头
            String tagHeader = genTagHeader(eFileTag);
            bufferedWriteByLine(bw, tagHeader);

            // 生成字段和字段描述
            String[] columnArr = genColumns(eFileTag);
            bufferedWriteByLine(bw, columnArr[0]);
            bufferedWriteByLine(bw, columnArr[1]);

            // 输入数据
            genTagDatas(bw, eFileTag);

            // 生成标签结束标志
            String tagEnder = genTagEnder(eFileTag);
            bufferedWriteByLine(bw, tagEnder);
        }
        // 结束
        bw.close();
        writer.close();
        return file;
    }


    public static String genHeader(EFileObject eFileObject) {
        return genHeader(eFileObject.getEFileInfo().getEntity(), eFileObject.getEFileInfo().getDataTime(), eFileObject.getEFileInfo().getType());
    }

    public static String genHeader(String entity, String dataTime, String type) {
        return "<! Entity='" + entity + "' dataTime='" + dataTime + "' type='" + type + "' !>";
    }

    public static String genTagHeader(EFileTag eFileTag) {
        return genTagHeader(eFileTag.getEFileTableInfo().getTableCode(), eFileTag.getEFileTableInfo().getTableName(), eFileTag.getEFileTableInfo().getPlanDate());
    }

    public static String genTagHeader(String tableCode, String tableName, String planDate) {
        return "<" + tableCode + "::" + tableName + " planDate='" + planDate + "' >";
    }

    public static String genTagEnder(EFileTag eFileTag) {
        return genTagEnder(eFileTag.getEFileTableInfo().getTableCode(), eFileTag.getEFileTableInfo().getTableName());
    }

    public static String genTagEnder(String tableCode, String tableName) {
        return "</" + tableCode + "::" + tableName + ">";
    }

    public static String[] genColumns(EFileTag eFileTag) {
        Map<String, String> columns = eFileTag.getColumns();
        Set<String> keys = columns.keySet();
        String[] columnArr = new String[2];
        StringBuilder columnKeys = new StringBuilder("@");
        StringBuilder columnDescs = new StringBuilder("//");
        for (String key : keys) {
            columnKeys.append(" ").append(key);
            columnDescs.append(" ").append(columns.get(key));
        }
        columnArr[0] = columnKeys.toString();
        columnArr[1] = columnDescs.toString();
        return columnArr;
    }

    public static String genColumnDescs(EFileTag eFileTag) {
        EFileTableInfo eFileTableInfo = eFileTag.getEFileTableInfo();
        return "</" + eFileTableInfo.getTableCode() + "::" + eFileTableInfo.getTableName() + ">";
    }


    public static void genTagDatas(BufferedWriter bw, EFileTag eFileTag) throws IOException {
        List<List<String>> datas = eFileTag.getDatas();

        for (List<String> data : datas) {
            // 每行数据对象
            StringBuilder dataStr = new StringBuilder("#");
            for (String value : data) {
                // 拼接每个数据
                dataStr.append(" ").append(value);
            }

            // 输出每行数据
            bufferedWriteByLine(bw, dataStr.toString());
        }
    }


    public static void main(String[] args) {
//        testParseEFile();

    }

    public static void testParseEFile() {
        try {
            EFile datas = new EFileUtil().parseEFile(new File("C:\\Users\\jdrag\\Desktop\\EFILE.txt"), null, new ArrayList<>(Arrays.asList("type", "planDate")));
            System.out.println(datas);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String genHeader(String entity, String dataTime, String type, String tyevalye) {
        return "<! Entity='" + entity + "'dataTime='" + dataTime + "' " + "type ='" + tyevalye + "' !>";
    }

    private static void bufferedWriteByLine(BufferedWriter bw, String line) throws IOException {
        bw.write(line);
        bw.newLine();
        bw.flush();
    }

    private static boolean createFile(String destFileName) {
        File file = new File(destFileName);
        if (file.exists()) {
            log.info("创建单个文件{}失败，目标文件已存在！", destFileName);
            return false;
        }
        if (destFileName.endsWith(File.separator)) {
            log.info("创建单个文件{}失败，目标文件不能为目录！", destFileName);
            return false;
        }
        if (!file.getParentFile().exists()) {
            log.info("目标文件所在目录不存在，准备创建它！");
            if (!file.getParentFile().mkdirs()) {
                log.info("创建目标文件所在目录失败！");
                return false;
            }
        }

        try {
            if (file.createNewFile()) {
                log.info("创建单个文件{}成功！", destFileName);
                return true;
            } else {
                log.info("创建单个文件{}失败！", destFileName);
                return false;
            }
        } catch (IOException e) {
            log.error("创建单个文件{}失败！", destFileName, e);
            return false;
        }
    }
}
