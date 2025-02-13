package com.jdragon.aggregation.report.localfile;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.spi.reporter.AbstractJobReporter;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class LocalFileReporter extends AbstractJobReporter {

    private String reportPath;

    public LocalFileReporter() {

    }

    @Override
    public void setConfiguration(Configuration configuration) {
        super.setConfiguration(configuration);
        String basePath = SystemConstants.HOME;
        this.reportPath = StringUtils.join(new String[]{basePath, "report", String.valueOf(getJobId())}, File.separator);
    }

    @Override
    public void report(RunStatus runStatus) {
        Communication communication = runStatus.getCommunication();
        Map<String, Number> counter = communication.getCounter();
        Map<String, Object> otherReportInfo = runStatus.getOtherReportInfo();

        String str = JSONObject.toJSONString(new Info(counter, otherReportInfo), SerializerFeature.PrettyFormat);
        FileUtil.mkParentDirs(reportPath);
        FileUtil.writeString(str, this.reportPath, StandardCharsets.UTF_8);
        log.info("report file: {} \n{}", this.reportPath, str);
    }

    @Override
    public void recovery(RunStatus runStatus) {
        boolean exist = FileUtil.exist(reportPath);
        FileUtil.mkParentDirs(reportPath);
        if (exist) {
            String string = FileUtil.readString(reportPath, StandardCharsets.UTF_8);
            Info info = JSONObject.parseObject(string, Info.class);
            log.info("load report file from [{}] \n {}", reportPath, info);
            runStatus.getOtherReportInfo().putAll(info.getOtherReportInfo());
        } else {
            log.info("report file: {} not exist", reportPath);
        }
    }

    @Data
    @AllArgsConstructor
    public static class Info {
        private Map<String, Number> counter;

        private Map<String, Object> otherReportInfo;
    }
}
