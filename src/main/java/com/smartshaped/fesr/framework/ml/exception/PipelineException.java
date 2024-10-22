package com.smartshaped.fesr.framework.ml.exception;

/**
 * Java class to manage exceptions inside Pipeline class and those that extends it.
 */
public class PipelineException extends Exception {

	public PipelineException(String message) {
        super("Exception in the ML Pipeline. Caused by: \n" + message);
    }
	
	public PipelineException(Throwable err) {
		super("Exception in the ML Pipeline. Caused by : \n" + err.getMessage(), err);
	}

    public PipelineException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
