package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StatisticsTest {

    @Mock
    Dataset<Row> df;
    @Mock
    Column column;

    @Test
    public void testGetDataFrameSummary() {

        when(df.col(anyString())).thenReturn(column);

        assertDoesNotThrow(() -> Statistics.getDataFrameSummary(df, "features"));
    }
}
