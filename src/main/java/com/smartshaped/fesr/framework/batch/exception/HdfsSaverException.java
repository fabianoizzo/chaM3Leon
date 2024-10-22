package com.smartshaped.fesr.framework.batch.exception;

public class HdfsSaverException extends Exception{

	private static final long serialVersionUID = -4328361625205944573L;
	
	public HdfsSaverException(String message) {
        super("Exception in the Hdfs Saver. Caused by: \n" + message);
    }
	
	public HdfsSaverException(Throwable err) {
		super("Exception in the Hdfs Saver. Caused by : \n" + err.getMessage(), err);
	}

    public HdfsSaverException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
