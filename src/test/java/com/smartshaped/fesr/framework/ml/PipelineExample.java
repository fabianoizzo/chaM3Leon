package com.smartshaped.fesr.framework.ml;

import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PipelineExample extends Pipeline {
    @Override
    public void start() {

    }

    @Override
    public void evaluatePredictions(Dataset<Row> predictions) {

    }

    @Override
    public void evaluateModel(Model<?> model) {

    }

    @Override
    public Model<?> readModelFromHDFS(String hdfsPath) {
        return null;
    }
}
