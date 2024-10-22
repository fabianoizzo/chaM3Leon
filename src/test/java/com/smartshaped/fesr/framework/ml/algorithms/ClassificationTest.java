package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ClassificationTest {

    @Mock
    Dataset<Row> df;
    @Mock
    Classifier<?, ?, ?> classifier;
    ParamMap[] paramMap;
    @Mock
    ParamMap paramMap1;
    String metricName;
    @Mock
    Estimator<?> estimator;
    @Mock
    Evaluator evaluator;

    @BeforeEach
    public void setup() {
        paramMap = new ParamMap[]{paramMap1};

        when(df.select(anyString())).thenReturn(df);
        when(df.distinct()).thenReturn(df);
    }

    @Test
    public void testRunBinaryClassifierBinary() {

        metricName = "areaUnderROC";

        when(df.count()).thenReturn(2L);

        try (MockedStatic<Prediction> mockedStatic = mockStatic(Prediction.class)) {

            mockedStatic.when(() -> Prediction.predictionWithTuning(df, estimator, 0.8, paramMap, evaluator,
                    "TrainValidationSplit", (float) 0.8)).thenReturn(df);

            assertDoesNotThrow(() -> Classification.runBinaryClassifier(df, classifier, paramMap, metricName,
                    "predictionCol", "labelCol", 0.8f, "CrossValidator",
                    3f));
        }
    }

    @Test
    public void testRunBinaryClassifierMultiClass() {

        metricName = "accuracy";

        when(df.count()).thenReturn(3L);

        try (MockedStatic<Prediction> mockedStatic = mockStatic(Prediction.class)) {

            mockedStatic.when(() -> Prediction.predictionWithTuning(df, estimator, 0.8, paramMap, evaluator,
                    "TrainValidationSplit", (float) 0.8)).thenReturn(df);

            assertDoesNotThrow(() -> Classification.runBinaryClassifier(df, classifier, paramMap, metricName,
                    "predictionCol", "labelCol", 0.8f, "CrossValidator",
                    3f));
        }
    }

    @Test
    public void testRunNonBinaryClassifierTest() {

        metricName = "areaUnderROC";

        when(df.count()).thenReturn(2L);

        try (MockedStatic<Prediction> mockedStatic = mockStatic(Prediction.class)) {

            mockedStatic.when(() -> Prediction.predictionWithTuning(df, estimator, 0.8, paramMap, evaluator,
                    "TrainValidationSplit", (float) 0.8)).thenReturn(df);

            assertDoesNotThrow(() -> Classification.runNonBinaryClassifier(df, classifier, paramMap, metricName,
                    "predictionCol", "labelCol", 0.8f, "CrossValidator",
                    3f));
        }
    }
}
