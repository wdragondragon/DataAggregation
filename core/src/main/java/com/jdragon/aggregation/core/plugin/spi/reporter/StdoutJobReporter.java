package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StdoutJobReporter extends AbstractJobReporter {
    @Override
    public void report(RunStatus runStatus) {
        log.info("report task status：\n{}", JSONObject.toJSONString(runStatus, SerializerFeature.PrettyFormat));
    }
}
