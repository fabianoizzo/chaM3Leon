package com.smartshaped.fesr.framework.preprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SimpleNonBinaryPreprocessor extends Preprocessor {

	private static final Logger logger = LogManager.getLogger(SimpleNonBinaryPreprocessor.class);
	
	@Override
	public Dataset<Row> preprocess(Dataset<Row> df) {
		
        logger.info("Processing non-binary stream");
        logger.debug("Binary flag is set to false");
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)", "timestamp");
        df.printSchema();
        logger.info("Selected columns for non-binary stream: key (as STRING), value (as STRING), timestamp");
        
		return df;
	}

}
