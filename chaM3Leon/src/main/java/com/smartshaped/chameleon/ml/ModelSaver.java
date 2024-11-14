package com.smartshaped.chameleon.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.smartshaped.chameleon.common.exception.CassandraException;
import com.smartshaped.chameleon.common.exception.ConfigurationException;
import com.smartshaped.chameleon.common.utils.CassandraUtils;
import com.smartshaped.chameleon.common.utils.TableModel;
import com.smartshaped.chameleon.ml.exception.ModelSaverException;
import com.smartshaped.chameleon.ml.utils.MLConfigurationUtils;

/**
 * Abstract class representing a saver for ml results.
 * <p>
 * This class provides a standard interface for saving models and predictions.
 * All model savers must extend this class and implement the methods.
 * <p>
 * This class is thread safe.
 */
public abstract class ModelSaver {

	private static final Logger logger = LogManager.getLogger(ModelSaver.class);

	private final String hdfsPath;
	private final TableModel tableModel;
	private final MLConfigurationUtils mlConfigurationUtils;

	protected ModelSaver() throws ConfigurationException {

		this.mlConfigurationUtils = MLConfigurationUtils.getMlConf();
		this.hdfsPath = mlConfigurationUtils.getModelDir();

		String modelName = mlConfigurationUtils.getModelClassName();
		this.tableModel = mlConfigurationUtils.createTableModel(modelName);
	}

	/**
	 * Save the model and predictions to HDFS and Cassandra respectively.
	 * <p>
	 * This method will first save the model to HDFS using the
	 * {@link #saveModelToHDFS(Model)} method. Then it will save the predictions to
	 * Cassandra using the {@link #savePredictionsToCassandra(Dataset)} method.
	 *
	 * @param pipeline the pipeline that contains the model and predictions
	 * @throws ModelSaverException    if any error occurs during the saving of the
	 *                                model
	 * @throws ConfigurationException if any error occurs during the configuration
	 * @throws CassandraException     if any error occurs while saving the
	 *                                predictions to Cassandra
	 */
	public void saveModel(Pipeline pipeline) throws ModelSaverException, ConfigurationException, CassandraException {

		Model<?> model = pipeline.getModel();
		Dataset<Row> predictions = pipeline.getPredictions();

		saveModelToHDFS(model);

		savePredictionsToCassandra(predictions);
	}

	/**
	 * This method takes an ML model and saves it to HDFS.
	 *
	 * @param model the model to be saved
	 * @throws ModelSaverException if any error occurs while saving the model
	 */
	private void saveModelToHDFS(Model<?> model) throws ModelSaverException {
		try {
			MLWritable writableModel = (MLWritable) model;

			MLWriter mlwriter = writableModel.write();
			mlwriter.overwrite().save(hdfsPath);

			logger.info("Model has been saved to HDFS");
		} catch (Exception e) {
			throw new ModelSaverException("Error while saving model to HDFS: " + hdfsPath, e);
		}
	}

	/**
	 * Saves the predictions to Cassandra.
	 * <p>
	 * This method first validates the TableModel by checking if the table exists in
	 * Cassandra. If the table does not exist, it creates the table using the given
	 * TableModel. Then, it saves the predictions to the Cassandra table using the
	 * {@link CassandraUtils#saveDF(Dataset, TableModel)} method.
	 * <p>
	 * This method will throw a {@link ConfigurationException} if the model is not
	 * valid and a {@link CassandraException} if any error occurs during the
	 * execution.
	 *
	 * @param predictions the predictions to be saved
	 * @throws ConfigurationException if the model is not valid
	 * @throws CassandraException     if any error occurs during the execution
	 */
	private void savePredictionsToCassandra(Dataset<Row> predictions)
			throws ConfigurationException, CassandraException {

		CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(mlConfigurationUtils);

		cassandraUtils.validateTableModel(tableModel);
		cassandraUtils.saveDF(predictions, tableModel);
	}
}
