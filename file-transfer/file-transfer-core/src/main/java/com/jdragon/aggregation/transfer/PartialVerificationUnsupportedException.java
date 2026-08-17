package com.jdragon.aggregation.transfer;

import java.io.IOException;

public class PartialVerificationUnsupportedException extends IOException {

    public PartialVerificationUnsupportedException(String message) {
        super(message);
    }
}
