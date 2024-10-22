package com.smartshaped.fesr.framework.batch.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.smartshaped.fesr.framework.batch.BatchUpdater;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.ConfigurationUtils;
import com.smartshaped.fesr.framework.preprocessing.Preprocessor;

public class BatchConfigurationUtils extends ConfigurationUtils {

    private static final String BATCH_KAFKA_SERVER = "batch.kafka.server";
    private static final String BATCH_KAFKA_INTERVAL = "batch.kafka.interval";
    private static final String BATCH_KAFKA_TOPICS = "batch.kafka.topics";
    private static final Logger logger = LogManager.getLogger(BatchConfigurationUtils.class);
    private static BatchConfigurationUtils configuration;

    private BatchConfigurationUtils() throws ConfigurationException {
        super();
        this.setConfRoot("batch.");
    }
    
    public Map<String,Preprocessor> getPreprocessors() throws ConfigurationException {
    	
        Iterator<String> keys = config.getKeys("batch.kafka.topics");
        
        logger.info("Reading configurations that starts with \"batch.kafka.topics\"");
        
        Map<String,Preprocessor> preprocessorMap = new HashMap<>();
        String fullKey;
        String preprocessorClassName;
        String topicName = "";
        while (keys.hasNext()) {
            fullKey = keys.next();
            
            if (fullKey.endsWith(".name")) {
            	topicName = config.getString(fullKey);
            }
            
            // only the configurations that ends with .class will be considered
            if (!topicName.isBlank() && fullKey.endsWith(".class")) {
            	
            	preprocessorClassName = config.getString(fullKey);
                
                if (preprocessorClassName != null && !preprocessorClassName.isBlank()) {
                    try {
                    	preprocessorMap.put(topicName, loadInstanceOf(preprocessorClassName, Preprocessor.class));
                    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                        throw new ConfigurationException("No valid " + Preprocessor.class + " binding exists", e);
                    } catch (Exception generic) {
                        throw new ConfigurationException("Could not instantiate " + Preprocessor.class + " due to exception", generic);
                    }
                }          
                
            }
        }
        
        return preprocessorMap;
    }
    
    public Preprocessor getPreprocessor(String topic, Map<String, String> kafkaConfig) throws ConfigurationException {
        
    	logger.info("Loading Preprocessor for topic: {}", topic);

        String preprocessorType = kafkaConfig.get(topic + ".preprocessor");
        
        if (preprocessorType == null || preprocessorType.trim().isEmpty()) {
            logger.info("No Preprocessor class configured");
            return null;
        }

        logger.info("Loading custom Preprocessor for topic: {}", topic);
        
        try {	
            return loadInstanceOf(preprocessorType, Preprocessor.class);
        } catch (Exception e) {
            throw new ConfigurationException("Failed to instantiate custom Preprocessor: " + preprocessorType, e);
        }
    }


    public static BatchConfigurationUtils getBatchConf() throws ConfigurationException {
        logger.info("Loading batch configuration");
        if (configuration == null) {
            logger.info("No previous batch configuration found, loading new configurations.");

            configuration = new BatchConfigurationUtils();
        }

        return configuration;
    }

    public Map<String, String> getKafkaConfig() throws ConfigurationException {
        logger.info("Loading Kafka configuration");
        Map<String, String> kafkaConfig = new HashMap<>();

        if (!config.containsKey(BATCH_KAFKA_SERVER)) {
            throw new ConfigurationException("Missing batch.kafka.server configuration key");
        }
        kafkaConfig.put("servers", config.getString(BATCH_KAFKA_SERVER));
        logger.info("Kafka server: {}", config.getString(BATCH_KAFKA_SERVER));
        
        if (!config.containsKey(BATCH_KAFKA_INTERVAL)) {
        	throw new ConfigurationException("Missing batch.kafka.interval configuration key");
        }
        kafkaConfig.put("intervalMs", config.getString(BATCH_KAFKA_INTERVAL));
        
        List<HierarchicalConfiguration<ImmutableNode>> topicList = config.childConfigurationsAt(BATCH_KAFKA_TOPICS);
        
        StringBuilder stringBuilder = new StringBuilder();
        
        
        String topicValue;
        String topic;
        String key;
        
        for(HierarchicalConfiguration<ImmutableNode> topicNode : topicList) {
        	topic = topicNode.getRootElementName();
        	key = BATCH_KAFKA_TOPICS + "." + topic;
        	topicValue = config.getString(key + ".name");
            kafkaConfig.put(topicValue + ".path", config.getString(key + ".path")); 
            kafkaConfig.put(topicValue + ".checkpoint", config.getString(key + ".checkpoint"));
        	stringBuilder.append(topicValue).append(",");
        	logger.info("Added Kafka topic: {}", topicValue);
        }

        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        logger.info("Kafka topics: {}", stringBuilder);
        kafkaConfig.put("topics", stringBuilder.toString());
        logger.info("Kafka configuration retrieved successfully");
        return kafkaConfig;
    }


    public BatchUpdater getBatchUpdater() throws ConfigurationException {
        logger.info("Loading BatchUpdater class.");
        String batchClassName = config.getString("layer.batch.batchClass");

        if (batchClassName == null || batchClassName.trim().isEmpty()) {
            logger.warn("No BatchUpdater class configured");
            return null;
        }

        try {
            BatchUpdater updater = loadInstanceOf(batchClassName, BatchUpdater.class);
            logger.info("BatchUpdater instantiated successfully: {}", batchClassName);
            return updater;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException("No valid " + BatchUpdater.class + " binding exists", e);
        } catch (Exception generic) {
            throw new ConfigurationException("Could not instantiate " + BatchUpdater.class + " due to exception", generic);
        }
    }


}
