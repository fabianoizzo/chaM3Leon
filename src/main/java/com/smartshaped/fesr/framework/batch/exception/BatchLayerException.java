package com.smartshaped.fesr.framework.batch.exception;

public class BatchLayerException extends Exception {

	private static final long serialVersionUID = 8384686595148208518L;
    
	public BatchLayerException(String message) {
        super("Exception in the Batch Layer. Caused by: \n" + message);
    }
	
	public BatchLayerException(Throwable err) {
		super("Exception in the Batch Layer. Caused by : \n" + err.getMessage(), err);
	}

    public BatchLayerException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
    	
}