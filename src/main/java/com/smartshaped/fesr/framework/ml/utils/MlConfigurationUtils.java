package com.smartshaped.fesr.framework.ml.utils;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.ConfigurationUtils;
import com.smartshaped.fesr.framework.ml.HDFSReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class that extends {@link ConfigurationUtils} for reading configuration files related to ML layer.
 */
public class MlConfigurationUtils extends ConfigurationUtils {

    private static final Logger logger = LogManager.getLogger(MlConfigurationUtils.class);


    private static MlConfigurationUtils configuration;

    private MlConfigurationUtils() throws ConfigurationException {
        super();
        this.setConfRoot("ml.");
    }

    /**
     * Static method to get a MlConfigurationUtils instance.
     * <p>
     * This method will create a new instance of MlConfigurationUtils if the
     * configuration is null, otherwise it will return the existing instance.
     *
     * @return MlConfigurationUtils instance.
     * @throws ConfigurationException if any error occurs while creating the
     *                                MlConfigurationUtils instance.
     */
    public static MlConfigurationUtils getMlConf() throws ConfigurationException {
        if (configuration == null) {
            configuration = new MlConfigurationUtils();
        }

        return configuration;
    }

    /**
     * Method to get HDFS path based on provided class name.
     *
     * @param className String representing class name.
     * @return String representing HDFS path.
     */
    public String getHDFSPath(String className) {
        logger.info("Getting HDFS path for class: {}", className);
        String defaultKey = "ml.hdfs.readers.default";
        String defaultValue = config.getString(defaultKey, "");
        logger.info("Default HDFS path: {}", defaultValue);

        Iterator<String> keys = config.getKeys("ml.hdfs.readers");

        logger.info("Reading configurations that starts with \"ml.hdfs.readers\"");

        int suffixLength = 6;    // length of .class
        String fullKey;
        String readerPrefix;
        String classConfigValue;
        String pathKey;

        while (keys.hasNext()) {
            fullKey = keys.next();

            if (fullKey.endsWith(".class")) {
                readerPrefix = fullKey.substring(0, fullKey.length() - suffixLength);
                classConfigValue = config.getString(fullKey);

                if (className.equals(classConfigValue)) {
                    pathKey = readerPrefix + ".path";
                    logger.info("Reader class {} found in configurations", className);
                    return config.getString(pathKey, defaultValue);
                }
            }
        }

        logger.info("Reader class {} not found in configuration file", className);
        logger.info("No specific HDFS path found for class {}, returning default: {}", className, defaultValue);
        return defaultValue;
    }

    /**
     * Returns a list of {@link HDFSReader} instances based on the configuration in the YAML file.
     * <p>
     * At least one reader must be defined in the configuration file.
     *
     * @return a list of {@link HDFSReader} instances
     * @throws ConfigurationException if no valid {@link HDFSReader} binding exists or if at least one reader is not defined
     */
    public List<HDFSReader> getHdfsReaders() throws ConfigurationException {

        Iterator<String> keys = config.getKeys("ml.hdfs.readers");

        logger.info("Reading configurations that starts with \"ml.hdfs.readers\"");

        List<HDFSReader> readerList = new ArrayList<>();
        String fullKey;
        String readerClassName;
        while (keys.hasNext()) {
            fullKey = keys.next();

            // only the configurations that ends with .class will be considered
            if (fullKey.endsWith(".class")) {

                readerClassName = config.getString(fullKey);

                if (readerClassName == null || readerClassName.trim().isEmpty()) {
                    throw new ConfigurationException("At least a reader must be defined in ml.hdfs.readers config");
                }

                try {
                    readerList.add(loadInstanceOf(readerClassName, HDFSReader.class));
                } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                    throw new ConfigurationException("No valid " + HDFSReader.class + " binding exists", e);
                } catch (Exception generic) {
                    throw new ConfigurationException("Could not instantiate " + HDFSReader.class + " due to exception", generic);
                }

            }
        }

        return readerList;
    }

    /**
     * Method that returns the model directory from configuration file.
     *
     * @return String representing the model directory.
     */
    public String getModelDir() {

        return config.getString("ml.hdfs.modelDir", "");
    }
}
