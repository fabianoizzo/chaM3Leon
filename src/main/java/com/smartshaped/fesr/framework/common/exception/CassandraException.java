package com.smartshaped.fesr.framework.common.exception;

/**
 * Exception thrown when an error occurs during the interaction with the Cassandra database.
 */
public class CassandraException extends Exception {

    public CassandraException(String message) {
        super("Exception related to Cassandra database. Caused by: \n" + message);
    }

    public CassandraException(Throwable err) {
        super("Exception related to Cassandra database. Caused by : \n" + err.getMessage(), err);
    }

    public CassandraException(String errMessage, Throwable err) {
        super(errMessage + "\n" + err.getMessage(), err);
    }
}
