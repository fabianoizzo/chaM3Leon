package com.smartshaped.fesr.framework.ml.exception;

/**
 * Java class to manage exceptions inside ModelSaver class and those that extends it.
 */
public class ModelSaverException extends Exception {
	
	public ModelSaverException(String message) {
        super("Exception in the Model Saver. Caused by: \n" + message);
    }
	
	public ModelSaverException(Throwable err) {
		super("Exception in the Model Saver. Caused by : \n" + err.getMessage(), err);
	}

    public ModelSaverException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
