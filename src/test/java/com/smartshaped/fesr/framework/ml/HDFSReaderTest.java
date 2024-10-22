package com.smartshaped.fesr.framework.ml;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.ml.exception.HDFSReaderException;
import com.smartshaped.fesr.framework.ml.utils.MlConfigurationUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class HDFSReaderTest {
	
	HDFSReader hdfsReader;
	@Mock
	MlConfigurationUtils configurationUtils;
	@Mock
	SparkSession sedona;
	@Mock
	Dataset<Row> dataFrame;
	@Mock
	DataFrameReader dataFrameReader;
	
	
	@Test
	void testConstructor() throws ConfigurationException {
		
		hdfsReader = new HDFSReader() {
			
		};
		
		assertNotNull(hdfsReader);
	}

    @Test
    void testStartSuccess() throws HDFSReaderException {
    	
    	when(sedona.read()).thenReturn(dataFrameReader);
    	when(sedona.read().parquet(anyString())).thenReturn(dataFrame);

    	hdfsReader = mock(HDFSReader.class, Mockito.CALLS_REAL_METHODS);
    	hdfsReader.setHdfsPath(anyString());
		
		hdfsReader.start(sedona);
		
		assertNotNull(hdfsReader);
    }
    
    @Test
    void testStartFailureRead() {
    	
        when(sedona.read()).thenReturn(dataFrameReader);
        doThrow(RuntimeException.class).when(dataFrameReader).parquet(anyString());

        hdfsReader = mock(HDFSReader.class, Mockito.CALLS_REAL_METHODS);

        assertThrows(HDFSReaderException.class, () -> {
        	hdfsReader.start(sedona);
        });
    }
    
    @Test
    void testStartFailureProcess() throws HDFSReaderException {
    	
    	hdfsReader = Mockito.mock(HDFSReader.class, Mockito.CALLS_REAL_METHODS);

		doNothing().when(hdfsReader).readRawData(sedona);
    	doThrow(HDFSReaderException.class).when(hdfsReader).processRawData(sedona);
    	
    	assertThrows(HDFSReaderException.class, () -> {
        	hdfsReader.start(sedona);
        });
    }
    
    @Test
    void testGettersAndSetters() {
  
    	hdfsReader = mock(HDFSReader.class, Mockito.CALLS_REAL_METHODS);
	  
    	String hdfsPath = "test";
	  
    	hdfsReader.setConfigurationUtils(configurationUtils);
    	hdfsReader.setDataframe(dataFrame);
	    hdfsReader.setHdfsPath(hdfsPath);
	  
	    assertEquals(configurationUtils, hdfsReader.getConfigurationUtils());
	    assertEquals(dataFrame, hdfsReader.getDataframe());
	    assertEquals(hdfsPath, hdfsReader.getHdfsPath());
    }
}
