package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class contains methods to execute mllib tuning algorithms.
 */
public class Tuning {

    /**
     * Method that calls a tuner algorithm.
     *
     * @param df         DataFrame.
     * @param estimator  Estimator instance; features column and prediction column must be already set.
     * @param paramMap   Parameter map containing parameter names and values to try.
     * @param evaluator  Evaluator instance; features column and prediction column must be already set.
     * @param tunerName  Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam Parameter for tuner, it can be trainRatio or numFolds.
     * @return Fine-tuned model.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Model<?> applyTuner(Dataset<Row> df, Estimator<?> estimator, ParamMap[] paramMap, Evaluator evaluator,
                                      String tunerName, float tunerParam) throws PipelineException {

        Model<?> model;

        if (tunerName.equals("TrainValidationSplit")) {
            model = applyTrainValidationSplit(df, estimator, paramMap, evaluator, tunerParam);
        } else if (tunerName.equals("CrossValidator")) {
            model = applyCrossValidator(df, estimator, paramMap, evaluator, (int) tunerParam);
        } else {
            throw new PipelineException("Invalid tuner name.");
        }

        return model;
    }

    /**
     * Method that calls the CrossValidator tuner.
     *
     * @param df        DataFrame.
     * @param estimator Estimator instance; features column and prediction column must be already set.
     * @param paramMap  Parameter map containing parameter names and values to try.
     * @param evaluator Evaluator instance; features column and prediction column must be already set.
     * @param numFolds  Number of folds for k-fold cross validation.
     * @return Fine-tuned model.
     */
    public static Model<?> applyCrossValidator(Dataset<Row> df, Estimator<?> estimator, ParamMap[] paramMap,
                                               Evaluator evaluator, int numFolds) {

        CrossValidator crossValidator = new CrossValidator().setEstimator(estimator).setEstimatorParamMaps(paramMap)
                .setEvaluator(evaluator).setNumFolds(numFolds);
        CrossValidatorModel crossValidatorModel = crossValidator.fit(df);

        return crossValidatorModel.bestModel();
    }

    /**
     * Method that calls the TrainValidationSplit tuner.
     *
     * @param df         DataFrame.
     * @param estimator  Estimator instance; features column and prediction column must be already set.
     * @param paramMap   Parameter map containing parameter names and values to try.
     * @param evaluator  Evaluator instance; features column and prediction column must be already set.
     * @param trainRatio Training set percentage.
     * @return Fine-tuned model.
     */
    public static Model<?> applyTrainValidationSplit(Dataset<Row> df, Estimator<?> estimator, ParamMap[] paramMap,
                                                     Evaluator evaluator, double trainRatio) {

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit().setEstimator(estimator)
                .setEstimatorParamMaps(paramMap).setEvaluator(evaluator).setTrainRatio(trainRatio);
        TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(df);

        return trainValidationSplitModel.bestModel();
    }
}
