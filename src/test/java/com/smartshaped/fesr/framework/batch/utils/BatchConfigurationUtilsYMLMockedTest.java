package com.smartshaped.fesr.framework.batch.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.hadoop.shaded.org.checkerframework.common.reflection.qual.ForName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.smartshaped.fesr.framework.batch.BatchUpdater;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;

@ExtendWith(MockitoExtension.class)

class BatchConfigurationUtilsYMLMockedTest {

	@Mock
	Configuration configuration = new YAMLConfiguration();

	@InjectMocks
	BatchConfigurationUtils batchConfig;

//	@Test
//	void getBatchUpdaterNullOrMissing() {
//		try {
//			doReturn("").when(configuration).getString("layer.batch.batchClass");
//			BatchUpdater batchUpdater = batchConfig.getBatchUpdater();
//			assertNull(batchUpdater);
//
//			doReturn(null).when(configuration).getString("layer.batch.batchClass");
//			batchUpdater = batchConfig.getBatchUpdater();
//			assertNull(batchUpdater);
//
//		} catch (Exception e) {
//			assert (false);
//
//		}
//
//	}
//
//	@Test
//	void getBatchUpdaterSuccessfull() {
//		try {
//			doReturn("com.smartshaped.fesr.framework.batch.utils.BatchUpdaterTestHelper").when(configuration)
//					.getString("layer.batch.batchClass");
//			BatchUpdater batchUpdater = batchConfig.getBatchUpdater();
//			assertNotEquals(null, batchUpdater);
//
//		} catch (Exception e) {
//			assert (false);
//
//		}
//
//	}
//
//	@Test
//	void getBatchUpdaterException() {
//			doReturn("obviouslyNotAClass").when(configuration).getString("layer.batch.batchClass");
//
//			assertThrows(ConfigurationException.class, () -> batchConfig.getBatchUpdater());
//			doReturn("com.smartshaped.fesr.framework.batch.utils.BatchConfigurationUtilsYMLMockedTest").when(configuration).getString("layer.batch.batchClass");
//
//			assertThrows(ConfigurationException.class, () -> batchConfig.getBatchUpdater());
//
//
//	}
//
//
//	@Test
//	void loadInstanceExceptions() {
//			doReturn("obviouslyNotAClass").when(configuration).getString("batchUpdater");
//
//			assertThrows(ConfigurationException.class, () -> batchConfig.getInstance("batchUpdater",BatchUpdater.class));
//			doReturn("com.smartshaped.fesr.framework.batch.utils.BatchConfigurationUtilsYMLMockedTest").when(configuration).getString("batchUpdater");
//
//			assertThrows(ConfigurationException.class, () -> batchConfig.getInstance("batchUpdater",BatchUpdater.class));
//			doReturn("").when(configuration).getString("batchUpdater");
//			assertThrows(ConfigurationException.class, () -> batchConfig.getInstance("batchUpdater",BatchUpdater.class));
//
//			doReturn(null).when(configuration).getString("batchUpdater");
//			assertThrows(ConfigurationException.class, () -> batchConfig.getInstance("batchUpdater",BatchUpdater.class));
//
//
//	}
//
//	@Test
//	void loadInstancesTest() {
//		List<String> stringList=new LinkedList<String>();
//			stringList.add("notAClass");
//			stringList.add("definetlyNotAClass.class");
//
//
//			doReturn(stringList.iterator()).when(configuration).getKeys(anyString());
//
//			assertThrows( ConfigurationException.class,() -> batchConfig.getInstances("batchUpdater",BatchUpdater.class));
//			doReturn("com.smartshaped.fesr.framework.batch.utils.BatchUpdaterTestHelper").when(configuration).getString(anyString());
//
//			stringList.clear();
//			stringList.add("aBatchUpdaterClass.class");
//			doReturn(stringList.iterator()).when(configuration).getKeys(anyString());
//
//			Map<String, Object> instances;
//			try {
//				instances = batchConfig.getInstances("batchUpdater",BatchUpdater.class);
//
//				assertTrue(!instances.isEmpty());
//			} catch (ConfigurationException e) {
//				System.out.println(e.getMessage());
//				assert(false);
//			}
//
//
//	}
//
//	@Test
//	void loadInstanceSuccessfull() {
//
//		try {
//			doReturn("com.smartshaped.fesr.framework.batch.utils.BatchUpdaterTestHelper").when(configuration)
//					.getString("batchUpdater");
//			BatchUpdater batchUpdater = batchConfig.getInstance("batchUpdater",BatchUpdater.class);
//			assertNotEquals(null, batchUpdater);
//
//		} catch (Exception e) {
//			assert (false);
//
//		}
//
//	}
//
//	@Test
//	void missingKafkaConfig() {
//
//		doReturn(false).when(configuration)
//		.containsKey("batch.kafka.server");
//		assertThrows(ConfigurationException.class, () -> batchConfig.getKafkaConfig());
//
//		doReturn(true).when(configuration)
//		.containsKey("batch.kafka.server");
//		doReturn(false).when(configuration)
//		.containsKey("batch.kafka.topics.topic1.name");
//
//		assertThrows(ConfigurationException.class, () -> batchConfig.getKafkaConfig());
//
//
//	}
//
//	@Test
//	void getSparkConfTest() {
//		List<String> keyList=new LinkedList<String>() ;
//		keyList.add("batch.null");
//		keyList.add("batch.empty");
//		keyList.add("batch.master");
//
//		doReturn(keyList.iterator()).when(configuration)
//		.getKeys(anyString());
//		doReturn(null).when(configuration)
//		.getString("batch.null");
//		doReturn("").when(configuration)
//		.getString("batch.empty");
//		doReturn("yarn").when(configuration)
//		.getString("batch.master");
//		assertDoesNotThrow(()->batchConfig.getSparkConf());
//
//	}
	

	


}
