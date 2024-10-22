package com.smartshaped.fesr.framework.speed.utils;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.ConfigurationUtils;
import com.smartshaped.fesr.framework.speed.SpeedUpdater;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class SpeedConfigurationUtils extends ConfigurationUtils {

    private static final String SPEED_KAFKA_SERVER = "Speed.kafka.server";
    private static final Logger logger = LogManager.getLogger(SpeedConfigurationUtils.class);
    private static SpeedConfigurationUtils configuration;

    private SpeedConfigurationUtils() throws ConfigurationException {
        super();
        this.setConfRoot("speed.");
    }

    public static SpeedConfigurationUtils getSpeedConf() throws ConfigurationException {
        logger.info("Loading speed configuration");
        if (configuration == null) {
            logger.info("No previous speed configuration found, loading new configurations.");

            configuration = new SpeedConfigurationUtils();
        }

        return configuration;
    }

    public Map<String, String> getKafkaConfig() throws ConfigurationException {
        logger.info("Loading Kafka configuration");
        Map<String, String> kafkaConfig = new HashMap<>();

        if (!config.containsKey(SPEED_KAFKA_SERVER)) {
            throw new ConfigurationException("Missing batch.kafka.server configuration key");
        }

        kafkaConfig.put("servers", config.getString(SPEED_KAFKA_SERVER));
        logger.info("Kafka server: {}", config.getString(SPEED_KAFKA_SERVER));

        if (!config.containsKey("batch.kafka.topics.topic1.name")) {
            throw new ConfigurationException("Missing batch.kafka.topics configuration");
        }

        StringBuilder stringBuilder = new StringBuilder();

        int topicNumber = 1;
        String key = "batch.kafka.topics.topic";
        String topicValue;
        while (config.containsKey(key + topicNumber + ".name")) {
            topicValue = config.getString(key + topicNumber + ".name");
            kafkaConfig.put(topicValue + ".preprocessor", config.getString(key + topicNumber + ".class"));
            kafkaConfig.put(topicValue + ".binary", config.getString(key + topicNumber + ".binary"));
            kafkaConfig.put(topicValue + ".path", config.getString(key + topicNumber + ".path"));
            stringBuilder.append(topicValue).append(",");
            logger.trace("Added Kafka topic: {}", topicValue);
            topicNumber++;
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        logger.info("Kafka topics: {}", stringBuilder);
        kafkaConfig.put("topics", stringBuilder.toString());
        logger.info("Kafka configuration retrieved successfully");
        return kafkaConfig;
    }


    public SpeedUpdater getSpeedUpdater() throws ConfigurationException {
        logger.info("Loading SpeedUpdater class.");
        String speedClassName = config.getString("layer.speed.speedClass");

        if (speedClassName == null || speedClassName.trim().isEmpty()) {
            throw new ConfigurationException("SpeedUpdater class is not configured");
        }

        try {
            SpeedUpdater updater = loadInstanceOf(speedClassName, SpeedUpdater.class);
            logger.info("BatchUpdater instantiated successfully: {}", speedClassName);
            return updater;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("No valid " + SpeedUpdater.class + " binding exists", e);
        } catch (Exception generic) {
            throw new ConfigurationException("Could not instantiate " + SpeedUpdater.class + " due to exception", generic);
        }

    }


}
