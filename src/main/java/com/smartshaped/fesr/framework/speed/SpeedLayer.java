package com.smartshaped.fesr.framework.speed;

import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.exception.KafkaConsumerException;
import com.smartshaped.fesr.framework.common.utils.KafkaConsumer;
import com.smartshaped.fesr.framework.speed.exception.SpeedLayerException;
import com.smartshaped.fesr.framework.speed.exception.SpeedUpdaterException;
import com.smartshaped.fesr.framework.speed.utils.SpeedConfigurationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Map;

public abstract class SpeedLayer {

    private static final Logger logger = LogManager.getLogger(SpeedLayer.class);

    private static final Logger log = LogManager.getLogger(com.smartshaped.fesr.framework.speed.SpeedLayer.class);
    private final SpeedConfigurationUtils configurationUtils;
    private final SparkSession sparkSession;
    SpeedUpdater speedUpdater;
    private Map<String, String> kafkaConfig;

    protected SpeedLayer() throws SpeedLayerException, ConfigurationException {

        this.configurationUtils = SpeedConfigurationUtils.getSpeedConf();
        log.info("Configuration correctly loaded");

        log.info("Loading configuration for spark session");
        SparkConf sparkConf = configurationUtils.getSparkConf();

        try {
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            log.info("Spark Session created");
        } catch (Exception e) {
            throw new SpeedLayerException("Error creating SparkSession", e);
        }

        logger.info("Loading kafka configurations.");
        this.kafkaConfig = configurationUtils.getKafkaConfig();

        logger.info("Loading Speed Updater.");
        this.speedUpdater = configurationUtils.getSpeedUpdater();

    }

    protected void start() throws ConfigurationException, KafkaConsumerException, CassandraException, SpeedLayerException {
        log.info("Starting SpeedLayer...");

        log.info("Starting kafkaToHdfs");
        Dataset<Row> df = KafkaConsumer.kafkaRead(kafkaConfig, sparkSession);
        log.info("KafkaToHdfs completed successfully");

        startSpeedUpdater(df);
        log.info("SpeedLayer completed successfully");

        try {
            sparkSession.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            throw new SpeedLayerException(e);
        }

        sparkSession.stop();
    }

    private void startSpeedUpdater(Dataset<Row> ds) throws CassandraException, SpeedUpdaterException {

        log.info("Starting speedUpdater.");
        speedUpdater.startUpdate(ds);
        log.info("speedUpdater completed.");

    }

}
