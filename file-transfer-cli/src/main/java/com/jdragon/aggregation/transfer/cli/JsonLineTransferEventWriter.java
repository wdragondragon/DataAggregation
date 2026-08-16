package com.jdragon.aggregation.transfer.cli;

import com.alibaba.fastjson.JSON;
import com.jdragon.aggregation.transfer.TransferEventListener;
import com.jdragon.aggregation.transfer.model.TransferEvent;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonLineTransferEventWriter implements TransferEventListener {

    private final PrintStream output;

    public JsonLineTransferEventWriter(PrintStream output) {
        this.output = output;
    }

    @Override
    public synchronized void onEvent(TransferEvent event) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("type", event.type().name());
        value.put("timestamp", event.timestamp().toString());
        value.put("runId", event.runId());
        value.put("itemId", event.itemId());
        value.put("itemStatus", event.itemStatus() == null ? null : event.itemStatus().name());
        value.put("transferredBytes", event.transferredBytes());
        value.put("totalBytes", event.totalBytes());
        value.put("attempt", event.attempt());
        value.put("message", event.message());
        value.put("errorCode", event.errorCode());
        value.put("details", event.details());
        output.println(JSON.toJSONString(value));
        output.flush();
    }
}
