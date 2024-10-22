package com.smartshaped.fesr.framework.speed.exception;


public class SpeedUpdaterException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SpeedUpdaterException(String message) {
        super(message);
    }

    public SpeedUpdaterException(String message, Throwable cause) {
        super(message, cause);
    }
}