package com.smartshaped.fesr.framework.ml.exception;

/**
 * Java class to manage exceptions inside HDFSReader class and those that extends it.
 */
public class HDFSReaderException extends Exception {
	
	public HDFSReaderException(String message) {
        super("Exception in the HDFS Reader. Caused by: \n" + message);
    }
	
	public HDFSReaderException(Throwable err) {
		super("Exception in the HDFS Reader. Caused by : \n" + err.getMessage(), err);
	}

    public HDFSReaderException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
