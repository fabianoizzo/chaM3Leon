package com.smartshaped.fesr.framework.common.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.smartshaped.fesr.framework.common.exception.CassandraException;
import com.smartshaped.fesr.framework.common.exception.ConfigurationException;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CassandraUtilsTest {

    @Mock
    ConfigurationUtils configurationUtils;
    @Mock
    CqlSessionBuilder cqlSessionBuilder;
    @Mock
    CqlSession cqlSession;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    BoundStatement boundStatement;
    @Mock
    ResultSet resultSet;
    @Mock
    TableModel tableModel;
    @Mock
    Dataset<Row> df;
    @Mock
    DataFrameWriter<Row> dfw;
    String node;
    String dataCenter;
    String keyspace;
    int port;

    @BeforeEach
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {

        Field instance = CassandraUtils.class.getDeclaredField("cassandraUtils");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    void testGetCassandraUtilsMissingNode() {

        node = "";
        port = 9042;
        dataCenter = "";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);

        assertThrows(ConfigurationException.class, () -> CassandraUtils.getCassandraUtils(configurationUtils));
    }

    @Test
    void testGetCassandraUtilsMissingDatacenter() {

        node = "test";
        port = 9042;
        dataCenter = "";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);

        assertThrows(ConfigurationException.class, () -> CassandraUtils.getCassandraUtils(configurationUtils));
    }

    @Test
    void testGetCassandraUtilsException() {

        node = "test";
        port = -9042;
        dataCenter = "test";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);

        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {

            mockedStatic.when(CqlSession::builder).thenReturn(cqlSessionBuilder);

            assertThrows(CassandraException.class, () -> CassandraUtils.getCassandraUtils(configurationUtils));
        }
    }

    @Test
    void testGetCassandraUtilsMissingKeyspace() {

        node = "test";
        port = 9042;
        dataCenter = "test";
        keyspace = "";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);
        when(configurationUtils.getCassandraKeySpaceName()).thenReturn(keyspace);

        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {

            mockedStatic.when(CqlSession::builder).thenReturn(cqlSessionBuilder);

            when(cqlSessionBuilder.addContactPoint(new InetSocketAddress(node, port)))
                    .thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.withLocalDatacenter(dataCenter)).thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.build()).thenReturn(cqlSession);

            assertThrows(ConfigurationException.class, () -> CassandraUtils.getCassandraUtils(configurationUtils));
        }
    }

    @Test
    void testGetCassandraUtilsQueryException() {

        node = "test";
        port = 9042;
        dataCenter = "test";
        keyspace = "test";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);
        when(configurationUtils.getCassandraKeySpaceName()).thenReturn(keyspace);

        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {

            mockedStatic.when(CqlSession::builder).thenReturn(cqlSessionBuilder);

            when(cqlSessionBuilder.addContactPoint(new InetSocketAddress(node, port)))
                    .thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.withLocalDatacenter(dataCenter)).thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.build()).thenReturn(cqlSession);
            when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
            when(preparedStatement.bind(any())).thenReturn(boundStatement);
            when(cqlSession.execute(boundStatement)).thenThrow(RuntimeException.class);

            assertThrows(CassandraException.class, () -> CassandraUtils.getCassandraUtils(configurationUtils));
        }
    }

    void createInstance() throws ConfigurationException, CassandraException {
        node = "test";
        port = 9042;
        dataCenter = "test";
        keyspace = "test";

        when(configurationUtils.getCassandraNode()).thenReturn(node);
        when(configurationUtils.getCassandraPort()).thenReturn(port);
        when(configurationUtils.getCassandraDataCenter()).thenReturn(dataCenter);
        when(configurationUtils.getCassandraKeySpaceName()).thenReturn(keyspace);

        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {

            mockedStatic.when(CqlSession::builder).thenReturn(cqlSessionBuilder);

            when(cqlSessionBuilder.addContactPoint(new InetSocketAddress(node, port)))
                    .thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.withLocalDatacenter(dataCenter)).thenReturn(cqlSessionBuilder);
            when(cqlSessionBuilder.build()).thenReturn(cqlSession);
            when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
            when(preparedStatement.bind(any())).thenReturn(boundStatement);
            when(cqlSession.execute(boundStatement)).thenReturn(resultSet);

            CassandraUtils.getCassandraUtils(configurationUtils);
        }
    }

    @Test
    void testGetCassandraUtilsSuccess() {

        assertDoesNotThrow(this::createInstance);
    }

    @Test
    void testValidateTableModel() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(), any())).thenReturn(boundStatement);
        when(cqlSession.execute(boundStatement)).thenReturn(resultSet);
        when(tableModel.getCreationQuery()).thenReturn("");

        assertDoesNotThrow(() -> cassandraUtils.validateTableModel(tableModel));
    }

    @Test
    void testSaveDFException() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        assertThrows(CassandraException.class, () -> cassandraUtils.saveDF(df, tableModel));
    }

    @Test
    void testSaveDFSuccess() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        when(tableModel.isGenerateUuid()).thenReturn(true);
        when(tableModel.getTableName()).thenReturn("test");
        when(df.withColumn("id", functions.expr("uuid()"))).thenReturn(df);
        when(df.write()).thenReturn(dfw);
        when(dfw.format(anyString())).thenReturn(dfw);
        when(dfw.options(anyMap())).thenReturn(dfw);
        when(dfw.mode((SaveMode) any())).thenReturn(dfw);

        assertDoesNotThrow(() -> cassandraUtils.saveDF(df, tableModel));
    }

    @Test
    void testClose() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        assertDoesNotThrow(cassandraUtils::close);
    }

    @Test
    void testExecuteSelect() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);
        assertDoesNotThrow(() -> cassandraUtils.executeSelect("test_table", Optional.of("test_field = 'test'")));
    }

    @Test
    void testExecuteInsert() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        Map<String, Object> values = new HashMap<>();
        values.put("test_field", "test");
        values.put("test_field2", "test2");

        assertDoesNotThrow(() -> cassandraUtils.executeInsert("test_table", values));
    }

    @Test
    void testExecuteUpdate() throws ConfigurationException, CassandraException {

        createInstance();
        CassandraUtils cassandraUtils = CassandraUtils.getCassandraUtils(configurationUtils);

        Map<String, Object> values = new HashMap<>();
        values.put("test_field", "test");

        assertDoesNotThrow(() -> cassandraUtils.executeUpdate("test_table", values,
                Optional.of("test_field = 'test'")));
    }
}
