package com.jdragon.aggregation.datasource.file;


import com.jdragon.aggregation.commons.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @Author JDragon
 * @Date 2022.05.09 下午 4:06
 * @Email 1061917196@qq.com
 * @Des:
 */
public class LocalFileHelper implements FileHelper {

    @Override
    public boolean exists(String path, String name) throws IOException {
        return new File(path).exists();
    }

    @Override
    public Set<String> listFile(String dir, String regex) {
        final File directory = new File(dir);
        if (!directory.isDirectory()) {
            throw new RuntimeException(dir + "不是文件夹");
        }
        final File[] files = directory.listFiles();
        if (files == null) {
            return new HashSet<>();
        }

        Set<String> fileList = new HashSet<>();
        Pattern pattern = Pattern.compile(regex);
        for (File file : files) {
            if (StringUtils.equalsAny(file.getName(), "..", ".")) {
                continue;
            }
            String name = file.getName();
            if (pattern.matcher(name).find()) {
                fileList.add(name);
            }
        }
        return fileList;
    }

    @Override
    public boolean isFile(String dir, String fileName) {
        final File d = new File(dir);
        File[] files = d.listFiles();
        if (files == null) {
            return false;
        }
        for (File file : files) {
            if (file.getName().equalsIgnoreCase(fileName)) {
                return file.isFile();
            }
        }
        return false;
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        final File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    @Override
    public void rm(String path) throws IOException {
        final File file = new File(path);
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            rmPath(file);
        } else {
            file.delete();
        }
    }

    @Override
    public boolean connect(Configuration configuration) {
        return false;
    }

    public void rmPath(File path) {
        final File[] files = path.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    rmPath(path);
                } else {
                    file.delete();
                }
            }
        }
        path.delete();
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean mv(String from, String to) throws Exception {
        return new File(from).renameTo(new File(to));
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        if (!exists(path, name)) {
            return null;
        }
        return Files.newInputStream(Paths.get(processingPath(path, name)));
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            boolean m = file.mkdirs();
            if (!m) {
                return null;
            }
        }
        return new FileOutputStream(processingPath(path, name), false);
    }

    @Override
    public void close() throws Exception {

    }
}
