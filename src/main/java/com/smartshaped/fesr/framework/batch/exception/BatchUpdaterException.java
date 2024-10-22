package com.smartshaped.fesr.framework.batch.exception;

public class BatchUpdaterException extends Exception {

	private static final long serialVersionUID = 7127888016514612395L;
	
	public BatchUpdaterException(String message) {
        super("Exception in the Batch Updater. Caused by: \n" + message);
    }
	
	public BatchUpdaterException(Throwable err) {
		super("Exception in the Batch Updater. Caused by : \n" + err.getMessage(), err);
	}

    public BatchUpdaterException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }

}
