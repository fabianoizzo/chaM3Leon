package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PipelinesTest {

    @Mock
    private Dataset<Row> df;
    private final PipelineStage[] stages = new PipelineStage[]{};
    private final ParamMap[] paramMaps = new ParamMap[]{};
    @Mock
    private Evaluator evaluator;
    @Mock
    Pipeline pipeline;
    @Mock
    PipelineModel pipelineModel;

    @Test
    public void testCreatePipelineModel() {

        try (MockedConstruction<Pipeline> mockedConstruction = Mockito.mockConstruction(Pipeline.class, (mock, context) -> {
            when(mock.setStages(stages)).thenReturn(pipeline);
        })) {
            when(pipeline.fit(any(Dataset.class))).thenReturn(pipelineModel);

            assertDoesNotThrow(() -> Pipelines.createPipelineModel(df, stages));
        }
    }

    @Test
    public void testCreatePipelineModelWithTuning() throws PipelineException {

        try (MockedStatic<Tuning> mockedStatic = Mockito.mockStatic(Tuning.class)) {
            mockedStatic.when(() -> Tuning.applyTuner(df, pipeline, paramMaps, evaluator,
                    "TrainValidationSplit", 0.8f)).thenReturn(pipelineModel);

            assertDoesNotThrow(() -> Pipelines.createPipelineModelWithTuning(df, stages, paramMaps, evaluator,
                    "TrainValidationSplit", 0.8f));
        }
    }
}
