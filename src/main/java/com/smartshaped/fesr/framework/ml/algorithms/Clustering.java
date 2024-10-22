package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.jetbrains.annotations.NotNull;


/**
 * This class contains methods to execute mllib clustering algorithms.
 */
public class Clustering {

    /**
     * Method that runs a clustering algorithm.
     *
     * @param df        DataFrame.
     * @param estimator A clustering estimator; features column and prediction column must be already set.
     * @return DataFrame with a new prediction column.
     */
    public static Dataset<Row> clustering(Dataset<Row> df, @NotNull Estimator<?> estimator) {

        Model<?> model = estimator.fit(df);
        df = model.transform(df);

        return df;
    }

    /**
     * Method that runs a clustering algorithm with grid-search.
     *
     * @param df         DataFrame.
     * @param estimator  A clustering estimator; features and prediction column must be already set.
     * @param paramMap   Param map containing parameter names and values to try.
     * @param evaluator  Evaluator instance; features and prediction column must be already set.
     * @param tunerName  Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam Parameter for tuner, it can be trainRatio or numFolds.
     * @return DataFrame with a new prediction column.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Dataset<Row> clusteringWithTuning(Dataset<Row> df, Estimator<?> estimator, ParamMap[] paramMap,
                                                    Evaluator evaluator, String tunerName,
                                                    float tunerParam) throws PipelineException {

        Model<?> model = Tuning.applyTuner(df, estimator, paramMap, evaluator, tunerName, tunerParam);
        df = model.transform(df);

        return df;
    }
}
