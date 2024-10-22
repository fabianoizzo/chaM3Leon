package com.smartshaped.fesr.framework.speed.exception;

public class SpeedLayerException extends Exception {
    public SpeedLayerException(String message) {
        super("Exception in the Speed Layer. Caused by: \n" + message);
    }

    public SpeedLayerException(Throwable err) {
        super("Exception in the Speed Layer. Caused by : \n" + err.getMessage(), err);
    }

    public SpeedLayerException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
