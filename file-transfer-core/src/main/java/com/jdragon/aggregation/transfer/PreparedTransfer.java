package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferPlan;
import com.jdragon.aggregation.transfer.model.TransferSpec;

public record PreparedTransfer(TransferSpec resolvedSpec, TransferPlan plan) {
}
