package com.smartshaped.fesr.framework.batch.preprocessing;

import java.security.AccessControlException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Preprocessor {

    private static final Logger logger = LogManager.getLogger(Preprocessor.class);

    protected String parquetPath;
    protected String dataClassName;

    protected boolean binary;

    /**
     * Process a given dataset and write it to a parquet file.
     *
     * This method will take a given dataset and write it to a parquet file at the location specified by
     * the {@code parquetPath} attribute. If the dataset is a binary stream, it will be processed as such.
     * If the dataset is not a binary stream, it will be processed as a non-binary stream.
     *
     * @param ds The dataset to be processed and written to parquet.
     * @throws StreamingQueryException Thrown if an error occurs while writing the dataset to parquet.
     * @throws TimeoutException Thrown if the write operation times out.
     */
    public void processAndSave(Dataset<Row> ds) throws StreamingQueryException, TimeoutException {
        logger.info("Starting processAndSave method");

        if (binary) {
            logger.info("Processing binary stream");
            logger.debug("Binary flag is set to true");
            ds = ds.selectExpr("CAST(key AS STRING)", "value", "timestamp");
            ds.printSchema();
            logger.info("Selected columns for binary stream: key (as STRING), timestamp");
        } else {
            logger.info("Processing non-binary stream");
            logger.debug("Binary flag is set to false");
            ds = ds.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)", "timestamp");
            logger.info("Selected columns for non-binary stream: key (as STRING), value (as STRING), timestamp");
        }

        logger.info("Preparing to write datastream to parquet");
        logger.debug("Parquet path: {}", parquetPath);

        String checkpointLocation = "/user/spark/checkpoint";
        logger.info("Using checkpoint location: {}", checkpointLocation);

        try {

            logger.info("Initiating writeStream operation");

            ds.write().format("parquet").mode("append").option("path", parquetPath)
                    .option("checkpointLocation", checkpointLocation).save();

            logger.info("WriteStream operation started successfully");

        } catch (AccessControlException e) {
            logger.error("Permission denied. Unable to write to path: {}. Error: {}", parquetPath, e.getMessage());
            logger.info("Please ensure that the user 'spark' has write permissions for the specified path.");
            logger.info("You may need to run: hdfs dfs -chmod -R 777 {}", parquetPath);
            throw new RuntimeException("Permission denied error in processAndSave", e);

        } catch (Exception e) {
            logger.error("Unexpected exception occurred during writeStream operation", e);
            throw new RuntimeException("Unexpected error in processAndSave", e);
        }

        logger.info("processAndSave method completed successfully");
    }

    public void setProperties(String path, boolean binary) {
        this.parquetPath = path;
        this.binary = binary;
    }

}
