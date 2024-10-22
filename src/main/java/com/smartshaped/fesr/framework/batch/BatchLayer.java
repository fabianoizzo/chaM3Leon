package com.smartshaped.fesr.framework.batch;

import com.smartshaped.fesr.framework.batch.exception.BatchLayerException;
import com.smartshaped.fesr.framework.batch.exception.BatchUpdaterException;
import com.smartshaped.fesr.framework.batch.exception.HdfsSaverException;
import com.smartshaped.fesr.framework.batch.utils.BatchConfigurationUtils;
import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.exception.KafkaConsumerException;
import com.smartshaped.fesr.framework.common.utils.KafkaConsumer;
import com.smartshaped.fesr.framework.preprocessing.Preprocessor;
import com.smartshaped.fesr.framework.preprocessing.exception.PreprocessorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Map;
import java.util.Objects;

public abstract class BatchLayer {

    private static final Logger logger = LogManager.getLogger(BatchLayer.class);

    private BatchConfigurationUtils configurationUtils;
    private SparkSession sparkSession;
    private Map<String, Preprocessor> preprocessors;
    private BatchUpdater batchUpdater;
    private Map<String, String> kafkaConfig;


    protected BatchLayer() throws ConfigurationException, BatchLayerException {

        this.configurationUtils = BatchConfigurationUtils.getBatchConf();
        logger.info("Configuration correctly loaded");

        logger.info("Loading configuration for spark session");
        SparkConf sparkConf = configurationUtils.getSparkConf();

        try {
            logger.info("Instantiating Spark Session");
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            logger.info("Spark Session created");
        } catch (Exception e) {
            throw new BatchLayerException("Error getting or creating SparkSession", e);
        }

        logger.info("Loading kafka configurations.");
        this.kafkaConfig = configurationUtils.getKafkaConfig();
        logger.info("Loading preprocessors.");
        this.preprocessors = configurationUtils.getPreprocessors();
        logger.info("Loading Batch Updater.");
        this.batchUpdater = configurationUtils.getBatchUpdater();
    }

    protected void start() throws ConfigurationException, KafkaConsumerException, BatchUpdaterException, CassandraException, PreprocessorException, HdfsSaverException, BatchLayerException {
        logger.info("Starting BatchLayer...");

        logger.info("Starting kafkaToHdfs");
        Dataset<Row> df = kafkaToHdfs();
        logger.info("KafkaToHdfs completed successfully");

        startBatchUpdater(df);
        logger.info("BatchLayer completed successfully");


        try {
            sparkSession.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            throw new BatchLayerException(e);
        }

        sparkSession.stop();
    }

    private void startBatchUpdater(Dataset<Row> ds) throws BatchUpdaterException, CassandraException, ConfigurationException {
        if (Objects.isNull(batchUpdater)) {
            logger.info("No batchUpdater found in configuration,skipping it...");
        } else {
            logger.info("Starting batchUpdater.");
            batchUpdater.startUpdate(ds);
            logger.info("BatchUpdater completed.");
        }
    }

    // Method responsible for retrieving data from kafkaTopics and saving them in
    // the corrisponding parquetFile

    private Dataset<Row> kafkaToHdfs() throws PreprocessorException, HdfsSaverException, KafkaConsumerException {

        String topics = kafkaConfig.get("topics");
        Long intervalMs = Long.valueOf(kafkaConfig.get("interval"));

        Dataset<Row> df = KafkaConsumer.kafkaRead(kafkaConfig, sparkSession);

        logger.info("Dataset subscribed to kafka.");

        //Preprocessor preprocessor;
        String[] topicArray = topics.split(",");
        Dataset<Row> tmpDf;

        logger.info("Start preprocessing data:");

        for (String topic : topicArray) {

            tmpDf = df.filter(df.col("topic").equalTo(topic));

            //tmpDf = rawDataframe(tmpDf, Boolean.parseBoolean(kafkaConfig.get(topic.concat(".binary"))));

            logger.info("Loading preprocessor for topic:{}", topic);

            if (!preprocessors.isEmpty()) {
                if (preprocessors.get(topic) != null) {
                    tmpDf = preprocessors.get(topic).preprocess(tmpDf);
                } else {
                    logger.info("No preprocessor defined for topic: {}", topic);
                }
            }

            logger.info("Save data for topic:{}", topic);

            HdfsSaver.save(tmpDf, kafkaConfig.get(topic.concat(".path")), kafkaConfig.get(topic.concat(".checkpoint")), intervalMs);


        }

        return df;

    }

}
