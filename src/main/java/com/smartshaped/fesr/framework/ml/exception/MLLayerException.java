package com.smartshaped.fesr.framework.ml.exception;

/**
 * Java class to manage exceptions inside MLLayer class and those that extends it.
 */
public class MLLayerException extends Exception {

	public MLLayerException(String message) {
        super("Exception in the ML Layer. Caused by: \n" + message);
    }
	
	public MLLayerException(Throwable err) {
		super("Exception in the ML Layer. Caused by : \n" + err.getMessage(), err);
	}

    public MLLayerException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
	
}
