package com.smartshaped.fesr.framework.batch.preprocessing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class PreprocessorTest {

    private Preprocessor preprocessor;


    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        preprocessor = new Preprocessor() {
            // Implementazione minima richiesta per la classe astratta
        };
    }

    @Test
    void testGettersAndSetters() {
        String parquetPath = "/path/to/parquet";
        String dataClassName = "TestDataClass";


        preprocessor.setParquetPath(parquetPath);
        preprocessor.setDataClassName(dataClassName);

        assertEquals(parquetPath, preprocessor.getParquetPath());
        assertEquals(dataClassName, preprocessor.getDataClassName());
    }

    @Test
    void testSetBinary() {
        preprocessor.setBinary(true);
        assertTrue(preprocessor.isBinary());

        preprocessor.setBinary(false);
        assertFalse(preprocessor.isBinary());
    }

    @Test
    void testPreprocessor() throws TimeoutException {
        Dataset<Row> data = mock(Dataset.class);
        DataFrameWriter dSW = mock(DataFrameWriter.class);
        StreamingQuery query = mock(StreamingQuery.class);
        preprocessor.setBinary(false);
        preprocessor.setParquetPath("parquet");
        when(data.selectExpr(anyString(), anyString(), anyString())).thenReturn(data);
        when(data.write()).thenReturn(dSW);
        when(dSW.format(anyString())).thenReturn(dSW);
        when(dSW.mode(anyString())).thenReturn(dSW);
        when(dSW.option(anyString(), anyString())).thenReturn(dSW);
        assertDoesNotThrow(() -> preprocessor.processAndSave(data));


        preprocessor.setBinary(true);
        assertDoesNotThrow(() -> preprocessor.processAndSave(data));

    }
}
