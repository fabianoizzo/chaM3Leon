package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ClusteringTest {

    @Mock
    Dataset<Row> df;
    @Mock
    Estimator<ModelExample> estimator;
    @Mock
    ModelExample modelExample;
    @Mock
    Model<?> model;
    @Mock
    ParamMap paramMap1;
    ParamMap[] paramMap = new ParamMap[]{paramMap1};
    @Mock
    Evaluator evaluator;

    @Test
    public void testClustering() {

        when(estimator.fit(df)).thenReturn(modelExample);

        assertDoesNotThrow(() -> Clustering.clustering(df, estimator));
    }

    @Test
    public void testClusteringWithTuning() {

        try (MockedStatic<Tuning> mockedStatic = mockStatic(Tuning.class)) {

            mockedStatic.when(() -> Tuning.applyTuner(df, estimator, paramMap, evaluator, "CrossValidator",
                    3f)).thenReturn(model);

            assertDoesNotThrow(() -> Clustering.clusteringWithTuning(df, estimator, paramMap, evaluator,
                    "CrossValidator", 3f));
        }
    }
}
