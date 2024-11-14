package com.smartshaped.chameleon.ml;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.smartshaped.chameleon.common.utils.CassandraUtils;
import com.smartshaped.chameleon.ml.exception.MLLayerException;
import com.smartshaped.chameleon.ml.utils.MLConfigurationUtils;

@ExtendWith(MockitoExtension.class)
class MLLayerTest {

	MLLayer mlLayer;
	List<HdfsReader> readerList;
	@Mock
	SparkSession sedona;
	@Mock
	Pipeline pipeline;
	@Mock
	ModelSaver modelSaver;
	@Mock
	MLConfigurationUtils configurationUtils;
	@Mock
	HdfsReader reader;
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
	void testConstructorFailure() {

		assertThrows(MLLayerException.class, CustomMlLayer::new);
	}

	@Test
    void testStartSuccess() {

        when(pipeline.getPredictions()).thenReturn(predictions);

        readerList = new ArrayList<>();
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
	void testStartSuccessNoOptionalParameters() {

		readerList = new ArrayList<>();
		readerList.add(reader);

		mlLayer.setReaderList(readerList);
		mlLayer.setSedona(sedona);

		try (MockedStatic<CassandraUtils> mockedStatic = mockStatic(CassandraUtils.class)) {

			mockedStatic.when(() -> CassandraUtils.getCassandraUtils(any())).thenReturn(cassandraUtils);

			assertDoesNotThrow(() -> mlLayer.start());
		}
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
