package com.smartshaped.fesr.framework.ml;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.CassandraUtils;
import com.smartshaped.fesr.framework.ml.exception.HDFSReaderException;
import com.smartshaped.fesr.framework.ml.exception.MLLayerException;
import com.smartshaped.fesr.framework.ml.exception.ModelSaverException;
import com.smartshaped.fesr.framework.ml.utils.CustomMlLayer;
import com.smartshaped.fesr.framework.ml.utils.MlConfigurationUtils;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class MLLayerTest {

    MLLayer mlLayer;
    List<HDFSReader> readerList;
    @Mock
    SparkSession sedona;
    @Mock
    Pipeline pipeline;
    @Mock
    ModelSaver modelSaver;
    @Mock
    MlConfigurationUtils configurationUtils;
    @Mock
    HDFSReader reader;
    @Mock
    StreamingQueryManager streamingQueryManager;
    @Mock
    SparkSession.Builder builder;
    @Mock
    Dataset<Row> predictions;
    @Mock
    CassandraUtils cassandraUtils;


    @BeforeEach
    void setUp() {
        mlLayer = mock(MLLayer.class, Mockito.CALLS_REAL_METHODS);
    }

    @Test
    void testConstructorSuccess() {

        try (MockedStatic<SedonaContext> mockedStatic = mockStatic(SedonaContext.class)) {

            mockedStatic.when(SedonaContext::builder).thenReturn(builder);
            when(builder.config(any(SparkConf.class))).thenReturn(builder);
            when(builder.getOrCreate()).thenReturn(sedona);

            mockedStatic.when(() -> SedonaContext.create(sedona)).thenReturn(sedona);

            assertDoesNotThrow(CustomMlLayer::new);
        }
    }

    @Test
    void testConstructorFailure() throws ConfigurationException, MLLayerException {

        assertThrows(MLLayerException.class, CustomMlLayer::new);
    }

    @Test
    void testStartSuccess() throws MLLayerException, HDFSReaderException, ModelSaverException, ConfigurationException {

        when(sedona.streams()).thenReturn(streamingQueryManager);
        when(pipeline.getPredictions()).thenReturn(predictions);

        readerList = new ArrayList<HDFSReader>();
        readerList.add(reader);

        mlLayer.setReaderList(readerList);
        mlLayer.setSedona(sedona);
        mlLayer.setPipeline(pipeline);
        mlLayer.setModelSaver(modelSaver);

        try (MockedStatic<CassandraUtils> mockedStatic = mockStatic(CassandraUtils.class)) {

            mockedStatic.when(() -> CassandraUtils.getCassandraUtils(any())).thenReturn(cassandraUtils);

            assertDoesNotThrow(() -> mlLayer.start());
        }
    }

    @Test
    void testStartSuccessNoOptionalParameters() throws MLLayerException, HDFSReaderException, ModelSaverException, ConfigurationException {

        when(sedona.streams()).thenReturn(streamingQueryManager);

        readerList = new ArrayList<HDFSReader>();
        readerList.add(reader);

        mlLayer.setReaderList(readerList);
        mlLayer.setSedona(sedona);

        try (MockedStatic<CassandraUtils> mockedStatic = mockStatic(CassandraUtils.class)) {

            mockedStatic.when(() -> CassandraUtils.getCassandraUtils(any())).thenReturn(cassandraUtils);

            assertDoesNotThrow(() -> mlLayer.start());
        }
    }

    @Test
    void testStartStreamingQueryException() throws StreamingQueryException {

        when(sedona.streams()).thenReturn(streamingQueryManager);
        doThrow(StreamingQueryException.class).when(streamingQueryManager).awaitAnyTermination();

        readerList = new ArrayList<HDFSReader>();
        readerList.add(mock(HDFSReader.class));

        mlLayer.setReaderList(readerList);
        mlLayer.setSedona(sedona);

        assertThrows(MLLayerException.class, () -> {
            mlLayer.start();
        });
    }

    @Test
    void testGettersAndSetters() {

        readerList = mock(List.class);

        mlLayer.setConfigurationUtils(configurationUtils);
        mlLayer.setModelSaver(modelSaver);
        mlLayer.setPipeline(pipeline);
        mlLayer.setReaderList(readerList);
        mlLayer.setSedona(sedona);

        assertEquals(configurationUtils, mlLayer.getConfigurationUtils());
        assertEquals(modelSaver, mlLayer.getModelSaver());
        assertEquals(pipeline, mlLayer.getPipeline());
        assertEquals(readerList, mlLayer.getReaderList());
        assertEquals(sedona, mlLayer.getSedona());
    }
}
