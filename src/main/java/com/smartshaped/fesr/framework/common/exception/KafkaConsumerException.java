package com.smartshaped.fesr.framework.common.exception;

public class KafkaConsumerException extends Exception {

    private static final long serialVersionUID = 7114969681718642282L;

    public KafkaConsumerException() {
    }

    public KafkaConsumerException(String message) {
        super(message);
    }

    public KafkaConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumerException(Throwable cause) {
        super(cause);
    }

}
