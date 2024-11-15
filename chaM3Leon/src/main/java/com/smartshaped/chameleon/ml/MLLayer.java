package com.smartshaped.chameleon.ml;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.smartshaped.chameleon.common.exception.CassandraException;
import com.smartshaped.chameleon.common.exception.ConfigurationException;
import com.smartshaped.chameleon.common.utils.CassandraUtils;
import com.smartshaped.chameleon.ml.exception.HdfsReaderException;
import com.smartshaped.chameleon.ml.exception.MLLayerException;
import com.smartshaped.chameleon.ml.exception.ModelSaverException;
import com.smartshaped.chameleon.ml.exception.PipelineException;
import com.smartshaped.chameleon.ml.utils.MLConfigurationUtils;

import lombok.Getter;
import lombok.Setter;

/**
 * Abstract class for the entry point of the machine learning layer;
 * all ml related components will be started from here and for this reason, extending this class is mandatory.
 */
@Getter
@Setter
public abstract class MLLayer {

    private static final Logger logger = LogManager.getLogger(MLLayer.class);

    private List<HdfsReader> readerList;
    private Pipeline pipeline;
    private ModelSaver modelSaver;
    private SparkSession sedona;
    private MLConfigurationUtils configurationUtils;


    protected MLLayer() throws ConfigurationException, MLLayerException {

        this.setConfigurationUtils(MLConfigurationUtils.getMlConf());
        this.setReaderList(this.configurationUtils.getHdfsReaders());
        this.setPipeline(this.configurationUtils.getPipeline());
        this.setModelSaver(this.configurationUtils.getModelSaver());

        SparkConf sedonaConf = this.configurationUtils.getSparkConf();

        try {
            logger.info("Instantiating Spark Session");

            SparkSession config = SedonaContext.builder()
                    .config(sedonaConf).getOrCreate();
            this.setSedona(SedonaContext.create(config));

            logger.info("Spark Session with Sedona created");

        } catch (Exception e) {
            throw new MLLayerException("Error getting or creating Sedona SparkSession", e);
        }
    }


    /**
     * Start the MLLayer.
     * <p>
     * This method will start all the HDFSReaders configured in the ml configuration.
     * After all the HDFSReaders have been started, it will start the configured Pipeline
     * and then save the model to HDFS using the configured ModelSaver.
     *
     * @throws MLLayerException       if any of the HDFSReaders fail to start
     * @throws HdfsReaderException    if any of the HDFSReaders have an error while running
     * @throws ModelSaverException    if the ModelSaver has an error while saving the model
     * @throws ConfigurationException if there is an error with the configuration
     * @throws CassandraException     if there is an error with Cassandra
     * @throws PipelineException      if there is an error with the Pipeline
     */
    public void start() throws MLLayerException, HdfsReaderException, ModelSaverException, ConfigurationException, CassandraException, PipelineException {

        List<Dataset<Row>> datasets = new ArrayList<>();

        for (HdfsReader reader : this.readerList) {
            logger.info("Starting {}", reader.getClass().getName());

            reader.start(this.sedona);
            datasets.add(reader.getDataframe());
        }

        if (this.pipeline != null) {

            pipeline.setDatasets(datasets);
            pipeline.start();

            if (this.modelSaver != null && pipeline.getPredictions() != null) {
                modelSaver.saveModel(pipeline);
            } else {
                logger.info("Model Saver skipped");
            }

        } else {
            logger.info("Pipeline skipped");
        }

        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);
        cassandraUtils.close();
        sedona.stop();
    }
}
