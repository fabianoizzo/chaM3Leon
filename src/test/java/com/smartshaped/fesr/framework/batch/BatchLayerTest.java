package com.smartshaped.fesr.framework.batch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.smartshaped.fesr.framework.batch.exception.BatchUpdaterException;
import com.smartshaped.fesr.framework.batch.preprocessing.Preprocessor;
import com.smartshaped.fesr.framework.batch.utils.BatchConfigurationUtils;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;

@ExtendWith(MockitoExtension.class)
class BatchLayerTest {

    @Mock
    private BatchConfigurationUtils configurationUtils;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private SparkSession.Builder sparkSessionBuilder;
    @Mock
    private BatchUpdater batchUpdater;
    @Mock
    private DataStreamReader dataStreamReader;
    @Mock
    private DataFrameReader dataFrameReader;
    @Mock
    private Dataset<Row> dataSetFrame;
    @Mock
    private Map<String, String> map;
    @Mock
    private SparkConf sparkConf;
    @Mock
    private Column column;
    @Mock
    private Preprocessor preprocessor;

//    @Test
//    void testStartSuccess() throws BatchUpdaterException, ConfigurationException, Exception {
//        BatchLayerTestClass batchLayer = new BatchLayerTestClass();
//
//        // Mock per SparkConf, SparkSession e DataStreamReader
//        when(configurationUtils.getSparkConf()).thenReturn(sparkConf);
//        doReturn(dataFrameReader).when(sparkSession).read();
//        when(dataFrameReader.format(anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.format(anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.option(anyString(), anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.load()).thenReturn(dataSetFrame);
//
//        // Mock per Kafka Config: restituisce un Map non nullo con 'topics' e 'servers'
//        when(configurationUtils.getKafkaConfig()).thenReturn(map);
//        when(map.get("topics")).thenReturn("testTopic1,testTopic2");
//        when(map.get("servers")).thenReturn("localhost:9092");
//
//        // Mock per SparkSession
//        when(sparkSessionBuilder.config(sparkConf)).thenReturn(sparkSessionBuilder);
//        when(sparkSessionBuilder.getOrCreate()).thenReturn(sparkSession);
//
//        // Mock per Dataset e Colonna
//        when(dataSetFrame.filter(any(Column.class))).thenReturn(dataSetFrame);
//        when(dataSetFrame.col(anyString())).thenReturn(column);
//        when(column.equalTo(any())).thenReturn(column);
//
//        // Mock per Preprocessor
//        when(configurationUtils.getPreprocessor(anyString(), any())).thenReturn(preprocessor);
//
//
//        // Mock statici
//        try (MockedStatic<BatchConfigurationUtils> mockedStatic = mockStatic(BatchConfigurationUtils.class)) {
//            mockedStatic.when(BatchConfigurationUtils::getBatchConf).thenReturn(configurationUtils);
//            try (MockedStatic<SparkSession> mockedStaticSpark = mockStatic(SparkSession.class)) {
//                mockedStaticSpark.when(SparkSession::builder).thenReturn(sparkSessionBuilder);
//
//                batchLayer.start();
//            }
//        }
//    }
//
//    @Test
//    void testConfigurationExceptionKafkaConfig() {
//
//        BatchLayerTestClass batchLayer = new BatchLayerTestClass();
//
//        try {
//            when(configurationUtils.getSparkConf()).thenReturn(sparkConf);
//            when(sparkSessionBuilder.config(sparkConf)).thenReturn(sparkSessionBuilder);
//            when(sparkSessionBuilder.getOrCreate()).thenReturn(sparkSession);
//            when(configurationUtils.getKafkaConfig()).thenThrow(ConfigurationException.class);
//        } catch (Exception e) {
//            assert (false);
//        }
//
//        try (MockedStatic<BatchConfigurationUtils> mockedStatic = mockStatic(BatchConfigurationUtils.class)) {
//            mockedStatic.when(BatchConfigurationUtils::getBatchConf).thenReturn(configurationUtils);
//            try (MockedStatic<SparkSession> mockedStaticSpark = mockStatic(SparkSession.class)) {
//                mockedStaticSpark.when(SparkSession::builder).thenReturn(sparkSessionBuilder);
//                assertThrows(ConfigurationException.class, () -> batchLayer.start());
//            }
//        }
//
//    }
//
//    @Test
//    void testStartElseBatchUpdater() throws ConfigurationException {
//
//        BatchLayerTestClass batchLayer = new BatchLayerTestClass();
//        BatchUpdaterTestClass batchUpdater1 = new BatchUpdaterTestClass();
//
//        // Mock per SparkConf, SparkSession e DataStreamReader
//        when(configurationUtils.getSparkConf()).thenReturn(sparkConf);
//        doReturn(dataFrameReader).when(sparkSession).read();
//        when(dataFrameReader.format(anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.format(anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.option(anyString(), anyString())).thenReturn(dataFrameReader);
//        when(dataFrameReader.load()).thenReturn(dataSetFrame);
//        when(configurationUtils.getKafkaConfig()).thenReturn(map);
//        when(map.get(anyString())).thenReturn("");
//        when(sparkSessionBuilder.config(sparkConf)).thenReturn(sparkSessionBuilder);
//        when(sparkSessionBuilder.getOrCreate()).thenReturn(sparkSession);
//        when(dataSetFrame.filter(column)).thenReturn(dataSetFrame);
//        when(dataSetFrame.col(anyString())).thenReturn(column);
//        when(column.equalTo(any())).thenReturn(column);
//        when(configurationUtils.getPreprocessor(anyString(), any())).thenReturn(preprocessor);
//        when(configurationUtils.getBatchUpdater()).thenReturn(batchUpdater1);
//
//        try (MockedStatic<BatchConfigurationUtils> mockedStatic = mockStatic(BatchConfigurationUtils.class)) {
//            mockedStatic.when(BatchConfigurationUtils::getBatchConf).thenReturn(configurationUtils);
//            try (MockedStatic<SparkSession> mockedStaticSpark = mockStatic(SparkSession.class)) {
//                mockedStaticSpark.when(SparkSession::builder).thenReturn(sparkSessionBuilder);
//                assertDoesNotThrow(() -> batchLayer.start());
//            }
//        }
//
//    }
//
//    private class BatchLayerTestClass extends BatchLayer {
//
//    }
//
//    private class BatchUpdaterTestClass extends BatchUpdater {
//
//        @Override
//        public void updateBatch() {
//
//        }
//
//    }

}
