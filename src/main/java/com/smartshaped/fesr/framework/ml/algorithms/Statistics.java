package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.stat.Summarizer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class that contains methods to extract usefull statistics from a dataFrame using mllib.
 */
public class Statistics {

    /**
     * Method that generates a summary of a dataFrame.
     *
     * @param df              DataFrame.
     * @param featuresColName Name of the features' column.
     * @return DataFrame containing summary stats.
     */
    public static Dataset<Row> getDataFrameSummary(Dataset<Row> df, String featuresColName) {
        Column featuresCol = df.col(featuresColName);

        return df.select(
                Summarizer.mean(featuresCol).alias("mean"),
                Summarizer.sum(featuresCol).alias("sum"),
                Summarizer.variance(featuresCol).alias("variance"),
                Summarizer.std(featuresCol).alias("std"),
                Summarizer.count(featuresCol).alias("count"),
                Summarizer.numNonZeros(featuresCol).alias("numNonZeros"),
                Summarizer.max(featuresCol).alias("max"),
                Summarizer.min(featuresCol).alias("min"),
                Summarizer.normL2(featuresCol).alias("normL2"),
                Summarizer.normL1(featuresCol).alias("normL1"));
    }
}
