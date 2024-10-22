package com.smartshaped.fesr.framework.ml;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.ml.exception.HDFSReaderException;
import com.smartshaped.fesr.framework.ml.utils.MlConfigurationUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Abstract class representing a HDFS reader.
 * <p>
 * This class provides a standard interface for all the HDFS readers. It defines the
 * methods to read raw data from HDFS and process it.
 * <p>
 * All the HDFSReaders must extend this class and implement the methods.
 */
@Setter
@Getter
public abstract class HDFSReader {

    private static final Logger logger = LogManager.getLogger(HDFSReader.class);
    private String hdfsPath;
    private Dataset<Row> dataframe;
    private MlConfigurationUtils configurationUtils;


    protected HDFSReader() throws ConfigurationException {

        this.configurationUtils = MlConfigurationUtils.getMlConf();
        String className = this.getClass().getName();
        this.hdfsPath = configurationUtils.getHDFSPath(className);

    }

    /**
     * Method to read raw data from HDFS and store it in a DataFrame
     *
     * @param sedona Spark session
     * @throws HDFSReaderException if any error occurs while reading from HDFS
     */
    public void readRawData(SparkSession sedona) throws HDFSReaderException {
        String filePath = this.hdfsPath;

        Dataset<Row> rawDF;

        try {
            // get DataFrame of binary data in HDFS
            rawDF = sedona.read().parquet(filePath);
            logger.info("Files in {} were read succesfully", filePath);
        } catch (Exception e) {
            throw new HDFSReaderException("Error while reading from HDFS", e);
        }

        this.dataframe = rawDF;
    }

    /**
     * This method starts the HDFSReader, reading the data from HDFS and processing it.
     *
     * @param sedona Spark session
     * @throws HDFSReaderException if any error occurs while reading or processing the data
     */
    public void start(SparkSession sedona) throws HDFSReaderException {

        try {
            this.readRawData(sedona);
        } catch (HDFSReaderException e) {
            throw new HDFSReaderException(e);
        }

        try {
            dataframe = this.processRawData(sedona);
        } catch (HDFSReaderException e) {
            throw new HDFSReaderException("Error processing Data", e);
        }
    }

    /**
     * Method to process data and make them suitable for machine learning.
     *
     * @param sedona Spark session.
     * @return Processed DataFrame.
     * @throws HDFSReaderException if any error occurs during data processing.
     */
    public Dataset<Row> processRawData(SparkSession sedona) throws HDFSReaderException {
        return this.dataframe;
    }
}
