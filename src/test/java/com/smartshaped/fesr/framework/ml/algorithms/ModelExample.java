package com.smartshaped.fesr.framework.ml.algorithms;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class ModelExample extends Model<ModelExample> {

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return null;
    }

    @Override
    public ModelExample copy(ParamMap extra) {
        return null;
    }

    @Override
    public String uid() {
        return "";
    }
}
