package com.smartshaped.fesr.framework.batch.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BatchUpdaterExceptionTest {

    @Test
    void testConstructorWithMessage() {
        String errorMessage = "Errore nel batch updater";
        BatchUpdaterException exception = new BatchUpdaterException(errorMessage);

        String expectedMessage = "Exception in the Batch Updater. Caused by: \n" + errorMessage;
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void testConstructorWithMessageAndThrowable() {
        String errorMessage = "Errore nel batch updater";
        Throwable cause = new RuntimeException("Causa originale");
        BatchUpdaterException exception = new BatchUpdaterException(errorMessage, cause);

        String expectedMessage = errorMessage + "\n" + cause.getMessage();
        assertEquals(expectedMessage, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testExceptionThrown() {
        assertThrows(BatchUpdaterException.class, () -> {
            throw new BatchUpdaterException("Test exception");
        });
    }

    @Test
    void testExceptionThrownWithCause() {
        Throwable cause = new IllegalArgumentException("Causa originale");
        BatchUpdaterException exception = assertThrows(BatchUpdaterException.class, () -> {
            throw new BatchUpdaterException("Test exception", cause);
        });
        assertEquals(cause, exception.getCause());
    }
}
