package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class that contains mllib frequent pattern mining algorithm.
 */
public class FPM {

    /**
     * Method that runs a frequent pattern mining algorithm with grid-search.
     *
     * @param df            DataFrame.
     * @param itemCol       Name of the item column.
     * @param predictionCol Name of the prediction column.
     * @param minSupport    Minimum support threshold.
     * @param minConfidence Minimum confidence threshold.
     * @return Test DataFrame with a new prediction column.
     */
    public static Dataset<Row> frequentPatternMining(Dataset<Row> df, String itemCol, String predictionCol,
                                                     int minSupport, double minConfidence) {

        FPGrowth fPGrowth = new FPGrowth().setItemsCol(itemCol).setMinSupport(minSupport).setMinConfidence(minConfidence).setPredictionCol(predictionCol);

        FPGrowthModel fpGrowthModel = fPGrowth.fit(df);

        df = fpGrowthModel.transform(df);

        return df;
    }
}
