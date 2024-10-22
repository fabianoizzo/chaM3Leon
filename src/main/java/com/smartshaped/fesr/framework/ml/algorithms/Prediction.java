package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;

/**
 * This class contains methods to execute generic mllib prediction algorithms.
 */
public class Prediction {

    /**
     * Method that runs a prediction algorithm.
     *
     * @param df         DataFrame.
     * @param estimator  A prediction estimator; features, prediction and actual column must be already set.
     * @param trainRatio Percentage of data to use for train set.
     * @return Test DataFrame with a new prediction column.
     */
    public static Dataset<Row> prediction(Dataset<Row> df, Estimator<?> estimator, double trainRatio) {

        // creation of train and test set
        double testRatio = 1 - trainRatio;

        Dataset<Row>[] trainTest = df.randomSplit(new double[]{trainRatio, testRatio});
        Dataset<Row> train = trainTest[0];
        Dataset<Row> test = trainTest[1];

        Model<?> model = estimator.fit(train);
        test = model.transform(test);

        return test;
    }

    /**
     * Method that runs a prediction algorithm with grid-search.
     *
     * @param df         DataFrame.
     * @param estimator  A prediction estimator; features, prediction and actual column must be already set.
     * @param trainRatio Percentage of data to use for train set.
     * @param paramMap   Param map containing parameter names and values to try.
     * @param evaluator  A prediction estimator; prediction and label column must be already set.
     * @param tunerName  Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam Parameter for tuner, it can be trainRatio or numFolds.
     * @return Test DataFrame with a new prediction column.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Dataset<Row> predictionWithTuning(Dataset<Row> df, Estimator<?> estimator, double trainRatio,
                                                    ParamMap[] paramMap, Evaluator evaluator, String tunerName,
                                                    float tunerParam) throws PipelineException {

        double testRatio = 1 - trainRatio;

        Dataset<Row>[] trainTest = df.randomSplit(new double[]{trainRatio, testRatio});
        Dataset<Row> train = trainTest[0];
        Dataset<Row> test = trainTest[1];

        Model<?> model = Tuning.applyTuner(train, estimator, paramMap, evaluator, tunerName, tunerParam);
        test = model.transform(test);

        return test;
    }
}
