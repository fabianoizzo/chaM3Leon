package com.smartshaped.fesr.framework.ml;

import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PipelineTest {

    String path;
    @Mock
    Model<?> model;
    @Mock
    Dataset<Row> predictions;
    PipelineExample pipeline;

    @BeforeEach
    public void setup() {
        pipeline = new PipelineExample();
        path = "path";
    }

    @Test
    void testStart() {
        assertDoesNotThrow(pipeline::start);
    }

    @Test
    void testEvaluatePredictions() {
        assertDoesNotThrow(() -> pipeline.evaluatePredictions(predictions));
    }

    @Test
    void testEvaluateModel() {
        assertDoesNotThrow(() -> pipeline.evaluateModel(model));
    }

    @Test
    void testReadModelFromHDFS() {
        assertDoesNotThrow(() -> pipeline.readModelFromHDFS(path));
    }

    @Test
    void testHdfsFileAlreadyExist() {
        assertDoesNotThrow(() -> pipeline.hdfsFileAlreadyExist(path));
    }
}
