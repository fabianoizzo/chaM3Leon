package com.smartshaped.fesr.framework.batch.exception;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BatchExceptionTest {

    @Test
    void testBatchExceptionWithThrowable() {
        Throwable cause = new RuntimeException("Errore di runtime");
        BatchException exception = new BatchException(cause);

        assertAll(
            () -> assertNotNull(exception),
            () -> assertTrue(exception.getMessage().contains("Exception in the Batch Layer")),
            () -> assertTrue(exception.getMessage().contains("Errore di runtime")),
            () -> assertEquals(cause, exception.getCause())
        );
    }

    @Test
    void testBatchExceptionWithMessageAndThrowable() {
        String errorMessage = "Errore nel processo batch";
        Throwable cause = new IllegalArgumentException("Argomento non valido");
        BatchException exception = new BatchException(errorMessage, cause);

        assertAll(
            () -> assertNotNull(exception),
            () -> assertTrue(exception.getMessage().contains(errorMessage)),
            () -> assertTrue(exception.getMessage().contains("Argomento non valido")),
            () -> assertEquals(cause, exception.getCause())
        );
    }

    @Test
    void testBatchExceptionThrown() {
        assertThrows(BatchException.class, () -> {
            throw new BatchException(new RuntimeException("Test exception"));
        });
    }
}