package com.smartshaped.chameleon.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.smartshaped.chameleon.batch.exception.BatchUpdaterException;
import com.smartshaped.chameleon.common.exception.CassandraException;
import com.smartshaped.chameleon.common.exception.ConfigurationException;

public class BatchUpdaterTestClass extends BatchUpdater {

	public BatchUpdaterTestClass() throws ConfigurationException, CassandraException {
		super();
	}

	@Override
	public Dataset<Row> updateBatch(Dataset<Row> df, SparkSession sparkSession) throws BatchUpdaterException {
		return df;
	}

}
