package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class RecommendationTest {

    @Mock
    Dataset<Row> df;
    @Mock
    ALS als;
    @Mock
    RegressionEvaluator regressionEvaluator;
    ParamMap[] paramMap = new ParamMap[]{mock(ParamMap.class)};

    @Test
    public void testRecommendation() {

        try (MockedStatic<Prediction> mockedStatic = mockStatic(Prediction.class)) {

            mockedStatic.when(() -> Prediction.predictionWithTuning(df, als, 0.8, paramMap,
                    regressionEvaluator, "TrainTestSplit", 0.8F)).thenReturn(df);

            assertDoesNotThrow(() -> Recommendation.recommendation(df, "user", "item", "rating",
                    "prediction", 0.8, paramMap, "rmse", "TrainTestSplit",
                    0.8F));
        }
    }
}
