package com.smartshaped.fesr.framework.batch;

import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import com.smartshaped.fesr.framework.batch.exception.HdfsSaverException;

public class HdfsSaver {

	private static final Logger logger = LogManager.getLogger(HdfsSaver.class);
	
	public static void save(Dataset<Row> ds, String parquetPath, String checkpointLocation, Long intervalMs) throws HdfsSaverException {
		
        logger.info("Initiating writeStream operation");

        try {
			ds.writeStream().format("parquet").outputMode(OutputMode.Append()).option("path", parquetPath)
			        .option("checkpointLocation", checkpointLocation).trigger(Trigger.ProcessingTime(intervalMs)).start();
		} catch (TimeoutException e) {
			throw new HdfsSaverException(e);
		}

        logger.info("WriteStream operation started successfully");
        
	}
}
