package com.smartshaped.fesr.framework.preprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.smartshaped.fesr.framework.preprocessing.exception.PreprocessorException;

public abstract class Preprocessor {

	public abstract Dataset<Row> preprocess(Dataset<Row> df) throws PreprocessorException;
}
