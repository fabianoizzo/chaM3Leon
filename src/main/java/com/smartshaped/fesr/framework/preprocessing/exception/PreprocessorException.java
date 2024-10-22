package com.smartshaped.fesr.framework.preprocessing.exception;

public class PreprocessorException extends Exception{

	private static final long serialVersionUID = 8730632671538489784L;
	
	public PreprocessorException(String message) {
        super("Exception in the Preprocessor. Caused by: \n" + message);
    }
	
	public PreprocessorException(Throwable err) {
		super("Exception in the Preprocessor. Caused by : \n" + err.getMessage(), err);
	}

    public PreprocessorException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
