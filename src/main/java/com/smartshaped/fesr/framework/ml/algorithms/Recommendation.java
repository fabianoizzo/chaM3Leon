package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;

/**
 * This class contains methods to execute mllib recommendation algorithms.
 */
public class Recommendation {

    /**
     * Method that runs a recommendation algorithm with grid-search.
     *
     * @param df         DataFrame.
     * @param userCol    Name of the user column.
     * @param itemCol    Name of the item column.
     * @param ratingCol  Name of the rating column.
     * @param predictionCol Name of the prediction column.
     * @param trainRatio Percentage of data to use for train set.
     * @param paramMap   Param map containing parameter names and values to try.
     * @param metricName Name of used metric for evaluator.
     * @param tunerName  Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam Parameter for tuner, it can be trainRatio or numFolds.
     * @return Test DataFrame with a new prediction column.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Dataset<Row> recommendation(Dataset<Row> df, String userCol, String itemCol, String ratingCol,
                                              String predictionCol, double trainRatio, ParamMap[] paramMap,
                                              String metricName, String tunerName,
                                              float tunerParam) throws PipelineException {

        ALS als = new ALS()
                .setUserCol(userCol)
                .setItemCol(itemCol)
                .setRatingCol(ratingCol)
                .setPredictionCol(predictionCol);

        RegressionEvaluator regressionEvaluator = new RegressionEvaluator().setLabelCol(ratingCol)
                .setPredictionCol(predictionCol)
                .setMetricName(metricName);

        Dataset<Row> test = Prediction.predictionWithTuning(df, als, trainRatio, paramMap, regressionEvaluator, tunerName,
                tunerParam);

        return test;
    }
}
