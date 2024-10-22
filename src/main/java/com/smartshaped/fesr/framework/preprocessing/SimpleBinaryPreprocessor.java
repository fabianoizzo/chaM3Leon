package com.smartshaped.fesr.framework.preprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SimpleBinaryPreprocessor extends Preprocessor {

	private static final Logger logger = LogManager.getLogger(SimpleBinaryPreprocessor.class);
	
	@Override
	public Dataset<Row> preprocess(Dataset<Row> df) {
		
        logger.info("Processing binary stream");
        logger.debug("Binary flag is set to true");
        df = df.selectExpr("CAST(key AS STRING)", "value", "timestamp");
        df.printSchema();
        logger.info("Selected columns for binary stream: key (as STRING), timestamp");
        
		return df;
	}

}
