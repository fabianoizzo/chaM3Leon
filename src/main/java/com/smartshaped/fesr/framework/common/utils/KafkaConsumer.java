package com.smartshaped.fesr.framework.common.utils;

import com.smartshaped.fesr.framework.common.exception.KafkaConsumerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class KafkaConsumer {

    private static final Logger log = LogManager.getLogger(KafkaConsumer.class);

    /**
     * Metodo generico per leggere da Kafka, supporta sia lettura streaming che batch.
     *
     * @param kafkaConfig  Mappa con le configurazioni di Kafka.
     * @param sparkSession Sessione Spark.
     * @return Dataset<Row>   Dataset letto da Kafka.
     * @throws KafkaConsumerException Se avviene un errore durante la lettura.
     */
    public static Dataset<Row> kafkaRead(Map<String, String> kafkaConfig, SparkSession sparkSession) throws KafkaConsumerException {

        String topics = kafkaConfig.get("topics");
        Dataset<Row> df;

        try {
            df = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaConfig.get("servers"))
                    .option("subscribe", topics)
                    .load();
            log.info("Kafka consumer in streaming mode.");

            df.printSchema();
            log.info("Dataset subscribed to Kafka on topics: {}", topics);

        } catch (Exception e) {
            throw new KafkaConsumerException("An error occurred during subscribing to Kafka. Cause: " + e.getMessage(), e);
        }

        return df;
    }
}
