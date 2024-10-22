package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FPMTest {

    @Mock
    private Dataset<Row> df;

    @Mock
    private FPGrowthModel fpGrowthModel;

    @Test
    public void testFrequentPatternMining() {
        try (MockedConstruction<FPGrowth> mockedConstruction = Mockito.mockConstruction(FPGrowth.class, (mock, context) -> {
            when(mock.setItemsCol("item")).thenReturn(mock);
            when(mock.setMinSupport(1)).thenReturn(mock);
            when(mock.setMinConfidence(0.8)).thenReturn(mock);
            when(mock.setPredictionCol("prediction")).thenReturn(mock);
            when(mock.fit(df)).thenReturn(fpGrowthModel);
        })) {
            when(fpGrowthModel.transform(df)).thenReturn(df);

            Dataset<Row> result = FPM.frequentPatternMining(df, "item", "prediction", 1, 0.8);

            assertEquals(df, result);
        }
    }
}