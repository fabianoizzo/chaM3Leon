package com.smartshaped.fesr.framework.ml;

import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.CassandraUtils;
import com.smartshaped.fesr.framework.ml.exception.HDFSReaderException;
import com.smartshaped.fesr.framework.ml.exception.MLLayerException;
import com.smartshaped.fesr.framework.ml.exception.ModelSaverException;
import com.smartshaped.fesr.framework.ml.utils.MlConfigurationUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class for the entry point of the machine learning layer;
 * all ml related components will be started from here and for this reason, extending this class is mandatory.
 */
@Getter
@Setter
public abstract class MLLayer {

    private static final Logger logger = LogManager.getLogger(MLLayer.class);

    private List<HDFSReader> readerList;
    private Pipeline pipeline;
    private ModelSaver modelSaver;
    private SparkSession sedona;
    private MlConfigurationUtils configurationUtils;


    protected MLLayer() throws ConfigurationException, MLLayerException {

        this.setConfigurationUtils(MlConfigurationUtils.getMlConf());
        this.setReaderList(this.configurationUtils.getHdfsReaders());

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
     * This method starts all the configured HDFSReaders and then starts the
     * pipeline. After that, it starts the model saver if configured.
     *
     * @throws MLLayerException       if any error occurs during the start of
     *                                the layer
     * @throws HDFSReaderException    if any error occurs during the start of
     *                                the HDFSReader
     * @throws ModelSaverException    if any error occurs during the start of
     *                                the model saver
     * @throws ConfigurationException if any error occurs during the
     *                                configuration of the layer
     * @throws CassandraException     if any error occurs while closing the
     *                                cassandra session
     */
    public void start() throws MLLayerException, HDFSReaderException, ModelSaverException, ConfigurationException, CassandraException {

        List<Dataset<Row>> datasets = new ArrayList<>();

        for (HDFSReader reader : this.readerList) {
            logger.info("Starting {}", reader.getClass().getName());

            reader.start(this.sedona);
            datasets.add(reader.getDataframe());
        }

        try {
            sedona.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            throw new MLLayerException(e);
        }

        if (this.pipeline != null) {
            pipeline.setDatasets(datasets);
            pipeline.start();
        } else {
            logger.info("Pipeline skipped");
        }

        if (this.modelSaver != null && pipeline.getPredictions() != null) {
            modelSaver.saveModel(pipeline);
        } else {
            logger.info("Model Saver skipped");
        }

        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);
        cassandraUtils.close();
        sedona.stop();
    }
}
