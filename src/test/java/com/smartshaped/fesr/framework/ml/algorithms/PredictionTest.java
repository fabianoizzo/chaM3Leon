package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PredictionTest {

    @Mock
    Dataset<Row> df;
    @Mock
    Estimator<ModelExample> estimator;
    @Mock
    ModelExample modelExample;
    @Mock
    Model<?> model;
    @Mock
    Evaluator evaluator;
    ParamMap[] paramMaps = new ParamMap[]{mock(ParamMap.class)};
    Dataset<Row>[] trainTest = new Dataset[]{mock(Dataset.class), mock(Dataset.class)};
    Double trainRatio = 0.8;
    Double testRatio = 1 - trainRatio;

    @BeforeEach
    public void setUp() {

        when(df.randomSplit(new double[]{trainRatio, testRatio})).thenReturn(trainTest);
    }

    @Test
    void testPrediction() {

        when(estimator.fit(trainTest[0])).thenReturn(modelExample);
        when(modelExample.transform(any(Dataset.class))).thenReturn(df);

        assertDoesNotThrow(() -> Prediction.prediction(df, estimator, trainRatio));
    }

    @Test
    void testPredictionWithTuning() throws PipelineException {

        when(model.transform(any(Dataset.class))).thenReturn(df);

        try (MockedStatic<Tuning> mockedStatic = mockStatic(Tuning.class)) {

            mockedStatic.when(() -> Tuning.applyTuner(trainTest[0], estimator, paramMaps, evaluator,
                    "TrainValidationSplit", 0.8f)).thenReturn(model);

            assertDoesNotThrow(() -> Prediction.predictionWithTuning(df, estimator, trainRatio, paramMaps, evaluator,
                    "TrainValidationSplit", 0.8f));
        }
    }
}
