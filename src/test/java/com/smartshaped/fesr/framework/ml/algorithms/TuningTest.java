package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TuningTest {

    @Mock
    Dataset<Row> df;
    @Mock
    Estimator<?> estimator;
    @Mock
    Evaluator evaluator;
    @Mock
    TrainValidationSplit trainValidationSplit;
    @Mock
    TrainValidationSplitModel trainValidationSplitModel;
    @Mock
    CrossValidator crossValidator;
    @Mock
    CrossValidatorModel crossValidatorModel;
    ParamMap[] paramMaps = new ParamMap[]{mock(ParamMap.class)};

    @Test
    void testApplyTunerTrainValidationSplit() throws PipelineException {

        try (MockedConstruction<TrainValidationSplit> mockedConstruction = Mockito.mockConstruction(TrainValidationSplit.class, (mock, context) -> {
            when(mock.setEstimator(estimator)).thenReturn(trainValidationSplit);
        })) {
            when(trainValidationSplit.setEstimatorParamMaps(paramMaps)).thenReturn(trainValidationSplit);
            when(trainValidationSplit.setEvaluator(evaluator)).thenReturn(trainValidationSplit);
            when(trainValidationSplit.setTrainRatio(0.8f)).thenReturn(trainValidationSplit);
            when(trainValidationSplit.fit(df)).thenReturn(trainValidationSplitModel);

            assertDoesNotThrow(() -> Tuning.applyTuner(df, estimator, paramMaps, evaluator,
                    "TrainValidationSplit", 0.8f));
        }
    }

    @Test
    void testApplyTunerCrossValidator() throws PipelineException {

        try (MockedConstruction<CrossValidator> mockedConstruction = Mockito.mockConstruction(CrossValidator.class, (mock, context) -> {
            when(mock.setEstimator(estimator)).thenReturn(crossValidator);
        })) {
            when(crossValidator.setEstimatorParamMaps(paramMaps)).thenReturn(crossValidator);
            when(crossValidator.setEvaluator(evaluator)).thenReturn(crossValidator);
            when(crossValidator.setNumFolds(4)).thenReturn(crossValidator);
            when(crossValidator.fit(df)).thenReturn(crossValidatorModel);

            assertDoesNotThrow(() -> Tuning.applyTuner(df, estimator, paramMaps, evaluator, "CrossValidator",
                    4));
        }
    }

    @Test
    void testApplyTunerInvalidTunerName() throws PipelineException {

        assertThrows(PipelineException.class, () -> Tuning.applyTuner(df, estimator, paramMaps, evaluator,
                "CrosValidator", 4));
    }
}
