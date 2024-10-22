package com.smartshaped.fesr.framework.ml;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;

/**
 * Abstract class representing a machine learning pipeline.
 * <p>
 * This class provides a standard interface for all the machine learning pipelines.
 * <p>
 * All the machine learning pipelines must extend this class and implement the methods.
*/
@Getter
@Setter
public abstract class Pipeline {

    private static final Logger logger = LogManager.getLogger(Pipeline.class);

    private List<Dataset<Row>> datasets;
    private Model<?> model;
    private Dataset<Row> predictions;


    public abstract void start();

    public abstract void evaluatePredictions(Dataset<Row> predictions);

    public abstract void evaluateModel(Model<?> model);

    public abstract Model<?> readModelFromHDFS(String hdfsPath);

    /**
     * Method to check if a given HDFS path already exists.
     *
     * @param hdfsPath String representing the path to check.
     * @return boolean indicating whether the path already exists.
     * @throws IOException if any error occurs while checking the path.
     */
    public boolean hdfsFileAlreadyExist(String hdfsPath) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path(hdfsPath);
        FileSystem fileSystem = FileSystem.get(configuration);

        return fileSystem.exists(path);
    }
}
