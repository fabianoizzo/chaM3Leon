package com.smartshaped.fesr.framework.speed;

import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.CassandraUtils;
import com.smartshaped.fesr.framework.common.utils.TableModel;
import com.smartshaped.fesr.framework.speed.exception.SpeedUpdaterException;
import com.smartshaped.fesr.framework.speed.utils.SpeedConfigurationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SpeedUpdater {

    private static final Logger logger = LogManager.getLogger(SpeedUpdater.class);


    private String modelName;
    private SpeedConfigurationUtils configurationUtils;
    private TableModel tableModel;
    private CassandraUtils cassandraUtils;


    protected SpeedUpdater() throws ConfigurationException, CassandraException {

        this.configurationUtils = SpeedConfigurationUtils.getSpeedConf();
        this.modelName = configurationUtils.getModelClassName();
        this.tableModel = configurationUtils.createTableModel(modelName);
        this.cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

    }

    public void startUpdate(Dataset<Row> df) throws SpeedUpdaterException, CassandraException {

        logger.info("Start Speed update...");
        Dataset<Row> updatedDF = updateSpeed(df);

        saveSpeed(updatedDF);
    }

    public abstract Dataset<Row> updateSpeed(Dataset<Row> df) throws SpeedUpdaterException;

    public void saveSpeed(Dataset<Row> df) throws CassandraException {

        logger.info("Validate table model");
        cassandraUtils.validateTableModel(tableModel);

        logger.info("Save updated dataframe");
        cassandraUtils.saveDF(df, tableModel);

        cassandraUtils.close();
    }

}
