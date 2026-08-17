package com.jdragon.aggregation.transfer;

public class TransferCanceledException extends Exception {

    public TransferCanceledException() {
        super("transfer was canceled");
    }
}
