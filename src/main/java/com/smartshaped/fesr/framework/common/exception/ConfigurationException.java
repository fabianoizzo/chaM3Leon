package com.smartshaped.fesr.framework.common.exception;

/**
 * Exception thrown when an error occurs inside ConfigurationUtils class and those that extends it.
 */
public class ConfigurationException extends Exception {

	public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
