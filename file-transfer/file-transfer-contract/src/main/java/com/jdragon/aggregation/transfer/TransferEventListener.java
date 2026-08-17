package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferEvent;

@FunctionalInterface
public interface TransferEventListener {

    TransferEventListener NOOP = event -> { };

    void onEvent(TransferEvent event);
}
