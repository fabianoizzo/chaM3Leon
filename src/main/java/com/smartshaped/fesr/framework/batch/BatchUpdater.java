package com.smartshaped.fesr.framework.batch;

import com.smartshaped.fesr.framework.batch.exception.BatchUpdaterException;
import com.smartshaped.fesr.framework.batch.utils.BatchConfigurationUtils;
import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.CassandraUtils;
import com.smartshaped.fesr.framework.common.utils.TableModel;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


@Getter
public abstract class BatchUpdater {

    private static final Logger logger = LogManager.getLogger(BatchUpdater.class);

    private String modelName;
    private BatchConfigurationUtils configurationUtils;
    private TableModel tableModel;
    private CassandraUtils cassandraUtils;


    protected BatchUpdater() throws ConfigurationException, CassandraException {

        this.configurationUtils = BatchConfigurationUtils.getBatchConf();
        this.modelName = configurationUtils.getModelClassName();
        this.tableModel = configurationUtils.createTableModel(modelName);
        this.cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

    }

    public void startUpdate(Dataset<Row> df) throws BatchUpdaterException, CassandraException {

        logger.info("Start batch update...");
        Dataset<Row> updatedDF = updateBatch(df);

        saveBatch(updatedDF);

    }

    public abstract Dataset<Row> updateBatch(Dataset<Row> df) throws BatchUpdaterException;

    public void saveBatch(Dataset<Row> df) throws CassandraException {

        logger.info("Validate table model");
        cassandraUtils.validateTableModel(tableModel);

        logger.info("Save updated dataframe");
        cassandraUtils.saveDF(df, tableModel);

        cassandraUtils.close();
    }


}
