package com.jdragon.aggregation.transfer;

public class TransferContractException extends IllegalArgumentException {

    private final String code;
    private final String field;

    public TransferContractException(String code, String field, String message) {
        super(message);
        this.code = code;
        this.field = field;
    }

    public TransferContractException(String code, String field, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.field = field;
    }

    public String code() {
        return code;
    }

    public String field() {
        return field;
    }
}
