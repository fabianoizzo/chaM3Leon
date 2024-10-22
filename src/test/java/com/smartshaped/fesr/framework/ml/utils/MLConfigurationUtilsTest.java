package com.smartshaped.fesr.framework.ml.utils;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import com.smartshaped.fesr.framework.common.utils.ConfigurationUtils;
import com.smartshaped.fesr.framework.ml.HDFSReader;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MLConfigurationUtilsTest {

    @InjectMocks
    MlConfigurationUtils mlConfigurationUtils;
    @Mock
    YAMLConfiguration config;
    Iterator<String> keys;
    String classKey;
    String className;
    @Mock
    ArrayList<HDFSReader> readerList;


    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        mlConfigurationUtils = mock(MlConfigurationUtils.class, Mockito.CALLS_REAL_METHODS);

        Field field = ConfigurationUtils.class.getDeclaredField("config");
        field.setAccessible(true);
        field.set(mlConfigurationUtils, config);

        classKey = "test.class";
        className = "test";

        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add(classKey);
        keys = arrayList.iterator();
    }

    @Test
    void testGetMlConf() throws ConfigurationException {

        assertNotNull(MlConfigurationUtils.getMlConf());
    }

    @Test
    void testGetHDFSPathSuccess() {

        when(config.getString(anyString(), anyString())).thenReturn("");
        when(config.getString(anyString())).thenReturn(className);
        when(config.getKeys(anyString())).thenReturn(keys);

        String result = mlConfigurationUtils.getHDFSPath(className);

        assertEquals("", result);
    }

    @Test
    void testGetHDFSPathFailure() {

        keys = mock(Iterator.class);

        when(config.getString(anyString(), anyString())).thenReturn("");
        when(config.getKeys(anyString())).thenReturn(keys);

        String result = mlConfigurationUtils.getHDFSPath(anyString());

        assertEquals("", result);
    }

    @Test
    void testGetHdfsReadersSuccess() throws ConfigurationException {

        className = CustomReader.class.getName();

        when(config.getKeys(anyString())).thenReturn(keys);
        when(config.getString(anyString())).thenReturn(className);

        assertDoesNotThrow(() -> mlConfigurationUtils.getHdfsReaders());
    }

    @Test
    void testGetHdfsReadersFailure1() {

        when(config.getKeys(anyString())).thenReturn(keys);

        assertThrows(ConfigurationException.class, () -> {
            mlConfigurationUtils.getHdfsReaders();
        });
    }

    @Test
    void testGetHdfsReadersFailure2() {

        className = "com.smartshaped.fesr.framework.ml.HDFSReader";

        when(config.getKeys(anyString())).thenReturn(keys);
        when(config.getString(anyString())).thenReturn(className);

        assertThrows(ConfigurationException.class, () -> {
            mlConfigurationUtils.getHdfsReaders();
        });
    }

    @Test
    void testGetHdfsReadersFailure3() {

        when(config.getKeys(anyString())).thenReturn(keys);
        when(config.getString(anyString())).thenReturn(className);

        assertThrows(ConfigurationException.class, () -> {
            mlConfigurationUtils.getHdfsReaders();
        });
    }

    @Test
    void testGetPolygonFilter() {
        assertDoesNotThrow(() -> mlConfigurationUtils.getPolygonFilter());
    }

    @Test
    void testGetTifMnipulationParams() {
        assertDoesNotThrow(() -> mlConfigurationUtils.getTifManipulationParams());
    }

    @Test
    void testGetModelDir() {
        assertDoesNotThrow(() -> mlConfigurationUtils.getModelDir());
    }
}
