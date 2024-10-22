package com.smartshaped.fesr.framework.ml.exception;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.smartshaped.fesr.framework.ml.exception.HDFSReaderException;

public class HDFSReaderExceptionTest {

	@Test
    void testHDFSReaderExceptionThrown() {
        assertThrows(HDFSReaderException.class, () -> {
            throw new HDFSReaderException("Test exception");
        });
    }
	
	@Test
    void testConstructorWithMessage() {
        String errorMessage = "Exception in the HDFS Reader";
        HDFSReaderException exception = new HDFSReaderException(errorMessage);

        String expectedMessage = "Exception in the HDFS Reader. Caused by: \n" + errorMessage;
        assertEquals(expectedMessage, exception.getMessage());
    }
	
	@Test
    void testExceptionThrownWithCause() {
        Throwable cause = new IllegalArgumentException("Original cause");
        HDFSReaderException exception = assertThrows(HDFSReaderException.class, () -> {
            throw new HDFSReaderException("Test exception", cause);
        });
        assertEquals(cause, exception.getCause());
    }
	
	@Test
    void testHDFSReaderExceptionWithThrowable() {
        Throwable cause = new RuntimeException("Runtime error");
        HDFSReaderException exception = new HDFSReaderException(cause);

        assertAll(
            () -> assertNotNull(exception),
            () -> assertTrue(exception.getMessage().contains("Exception in the HDFS Reader")),
            () -> assertTrue(exception.getMessage().contains("Runtime error")),
            () -> assertEquals(cause, exception.getCause())
        );
    }
	
	@Test
    void testHDFSReaderExceptionWithMessageAndThrowable() {
        String errorMessage = "Error when reading from hdfs";
        Throwable cause = new IllegalArgumentException("Invalid argument");
        HDFSReaderException exception = new HDFSReaderException(errorMessage, cause);

        assertAll(
            () -> assertNotNull(exception),
            () -> assertTrue(exception.getMessage().contains(errorMessage)),
            () -> assertTrue(exception.getMessage().contains("Invalid argument")),
            () -> assertEquals(cause, exception.getCause())
        );
    }
}
