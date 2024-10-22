package com.smartshaped.fesr.framework.batch.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.smartshaped.fesr.framework.batch.BatchUpdater;
import com.smartshaped.fesr.framework.common.utils.ConfigurationUtils;

@ExtendWith(MockitoExtension.class)
class BatchConfigurationUtilsTest {

	@Test
	void getBatchConfTestSuccessfull() {
		try {
			BatchConfigurationUtils config = BatchConfigurationUtils.getBatchConf();
			BatchConfigurationUtils config2 = BatchConfigurationUtils.getBatchConf();
			assertEquals(config, config2);
			BatchConfigurationUtils config3 = mock(BatchConfigurationUtils.class);
			assertNotEquals(config, config3);

		} catch (Exception e) {
			assert (false);
		}
	}

	@Test
	void getKafkaConfigTestSuccessful() {
		try {
			BatchConfigurationUtils config = BatchConfigurationUtils.getBatchConf();
			Map<String, String> kafkaConfig = config.getKafkaConfig();

			assertEquals(kafkaConfig.get("servers"), "kafkaservers");
			assertEquals(kafkaConfig.get("topics"), "topic1,topicTif");

			assertEquals(kafkaConfig.get("topicTif"), null);

		} catch (Exception e) {
			assert (false);
		}

	}

}
