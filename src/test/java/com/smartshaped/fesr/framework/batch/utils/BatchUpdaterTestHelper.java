package com.smartshaped.fesr.framework.batch.utils;

import com.smartshaped.fesr.framework.batch.BatchUpdater;
import com.smartshaped.fesr.framework.batch.exception.BatchUpdaterException;
import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class BatchUpdaterTestHelper extends BatchUpdater{

	public BatchUpdaterTestHelper() throws ConfigurationException, CassandraException {
        super();
    }

	@Override
	public Dataset<Row> updateBatch(Dataset<Row> df) throws BatchUpdaterException {
		return null;
	}
}
