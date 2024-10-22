package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class contains methods to generate and run mllib pipelines.
 */
public class Pipelines {

    /**
     * Method that generate a pipeline model based on provided stages.
     *
     * @param df     DataFrame.
     * @param stages Array of pipeline stages.
     * @return Generated model.
     */
    public static PipelineModel createPipelineModel(Dataset<Row> df, PipelineStage[] stages) {

        Pipeline pipeline = new Pipeline().setStages(stages);

        return pipeline.fit(df);
    }

    /**
     * Method that generate a fine-tuned pipeline model based on provided stages.
     *
     * @param df         DataFrame.
     * @param stages     Array of pipeline stages.
     * @param paramMap   Parameter map containing parameter names and values to try.
     * @param evaluator  Evaluator instance; features column and prediction column must be already set.
     * @param tunerName  Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam Parameter for tuner, it can be trainRatio or numFolds.
     * @return Generated model.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Model<?> createPipelineModelWithTuning(Dataset<Row> df, PipelineStage[] stages, ParamMap[] paramMap,
                                                         Evaluator evaluator, String tunerName,
                                                         float tunerParam) throws PipelineException {

        Pipeline pipeline = new Pipeline().setStages(stages);

        return Tuning.applyTuner(df, pipeline, paramMap, evaluator, tunerName, tunerParam);
    }
}
