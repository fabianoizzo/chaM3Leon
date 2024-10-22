package com.smartshaped.fesr.framework.ml.algorithms;

import com.smartshaped.fesr.framework.ml.exception.PipelineException;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class contains methods to execute mllib classification algorithms.
 */
public class Classification {

    /**
     * Method to execute classification algorithms that requires OVR when there are more than two classes.
     *
     * @param df            DataFrame.
     * @param classifier    A classification estimator; features, prediction and actual column must be already set.
     * @param paramMap      Param map containing parameter names and values to try.
     * @param metricName    Name of used metric for evaluator.
     * @param predictionCol Name of the prediction column.
     * @param labelCol      Name of the label column.
     * @param trainRatio    Percentage of data to use for train set.
     * @param tunerName     Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam    Parameter for tuner, it can be trainRatio or numFolds.
     * @return Test DataFrame with a new prediction column.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Dataset<Row> runBinaryClassifier(Dataset<Row> df, Classifier<?, ?, ?> classifier, ParamMap[] paramMap,
                                                   String metricName, String predictionCol, String labelCol,
                                                   float trainRatio, String tunerName,
                                                   float tunerParam) throws PipelineException {

        boolean isMulticlass = multiclassClassificationCheck(df, labelCol);
        Estimator<?> estimator = getUsableClassifier(classifier, isMulticlass);
        Evaluator evaluator = getClassificationEvaluator(isMulticlass, predictionCol, labelCol, metricName);

        df = Prediction.predictionWithTuning(df, estimator, trainRatio, paramMap, evaluator, tunerName, tunerParam);

        return df;
    }

    /**
     * Method to execute classification algorithms that doesn't require OVR when there are more than two classes.
     *
     * @param df            DataFrame.
     * @param classifier    A classification estimator; features, prediction and actual column must be already set.
     * @param paramMap      Param map containing parameter names and values to try.
     * @param metricName    Name of used metric for evaluator.
     * @param predictionCol Name of the prediction column.
     * @param labelCol      Name of the label column.
     * @param trainRatio    Percentage of data to use for train set.
     * @param tunerName     Name of chose tuner, the options are "TrainValidationSplit" and "CrossValidator".
     * @param tunerParam    Parameter for tuner, it can be trainRatio or numFolds.
     * @return Test DataFrame with a new prediction column.
     * @throws PipelineException if the name of the tuner is invalid.
     */
    public static Dataset<Row> runNonBinaryClassifier(Dataset<Row> df, Classifier<?, ?, ?> classifier,
                                                      ParamMap[] paramMap, String metricName, String predictionCol,
                                                      String labelCol, float trainRatio, String tunerName,
                                                      float tunerParam) throws PipelineException {

        boolean isMulticlass = multiclassClassificationCheck(df, labelCol);
        Evaluator evaluator = getClassificationEvaluator(isMulticlass, predictionCol, labelCol,
                metricName);

        df = Prediction.predictionWithTuning(df, classifier, trainRatio, paramMap, evaluator, tunerName, tunerParam);

        return df;
    }

    /**
     * Method that check if the label column contains more than two classes.
     *
     * @param df       DataFrame.
     * @param labelCol Name of the label column.
     * @return True if there are more than two classes.
     */
    public static boolean multiclassClassificationCheck(Dataset<Row> df, String labelCol) {
        long labelsNumber = df.select(labelCol).distinct().count();

        return labelsNumber > 2;
    }

    /**
     * Method that applies the one vs rest technique to a classifier if classification is
     *
     * @param classifier   Classifier instance; features, column and prediction column must be already set.
     * @param isMulticlass Boolean that indicates if the label field contains more than two classes.
     * @return The same classifier if isMulticlass is false, otherwise an OVR estimator based on the classifier.
     */
    public static Estimator<?> getUsableClassifier(Classifier<?, ?, ?> classifier, boolean isMulticlass) {

        Estimator<?> estimator;
        if (isMulticlass) {
            estimator = new OneVsRest().setClassifier(classifier);
        } else {
            estimator = classifier;
        }

        return estimator;
    }

    /**
     * Method that return the appropriate evaluator based on the number of classes.
     *
     * @param isMulticlass  Boolean that indicates if the label field contains more than two classes.
     * @param predictionCol Name of the prediction column.
     * @param labelCol      Name of the label column.
     * @param metricName    Name of used metric for evaluator.
     * @return A MulticlassClassificationEvaluator instance or a BinaryClassificationEvaluator instance.
     */
    public static Evaluator getClassificationEvaluator(boolean isMulticlass, String predictionCol, String labelCol,
                                                       String metricName) {

        Evaluator evaluator;
        if (isMulticlass) {
            evaluator = new MulticlassClassificationEvaluator().setPredictionCol(predictionCol).setLabelCol(labelCol)
                    .setMetricName(metricName);
        } else {
            evaluator = new BinaryClassificationEvaluator().setRawPredictionCol(predictionCol).setLabelCol(labelCol)
                    .setMetricName(metricName);
        }
        return evaluator;
    }
}
