package com.smartshaped.fesr.framework.batch;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BatchUpdaterTest {

    @Mock
    private CqlSession session;
    @Mock
    private CqlSessionBuilder cqlSessionBuilder;
    @Mock
    private CqlIdentifier identifier;


    private BatchUpdater batchUpdater;


//    @BeforeEach
//    public void setUp() {
//        batchUpdater = mock(BatchUpdater.class, Mockito.CALLS_REAL_METHODS);
//
//        when(cqlSessionBuilder.addContactPoint(new InetSocketAddress("", 0))).thenReturn(cqlSessionBuilder);
//        when(cqlSessionBuilder.withLocalDatacenter(anyString())).thenReturn(cqlSessionBuilder);
//        when(cqlSessionBuilder.withKeyspace(identifier)).thenReturn(cqlSessionBuilder);
//        when(cqlSessionBuilder.build()).thenReturn(session);
//
//    }
//
//    @Test
//    public void testConnect() {
//
//        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
//            mockedStatic.when(()->CqlSession.builder()).thenReturn(cqlSessionBuilder);
//            try (MockedStatic<CqlIdentifier> mockedStaticTwo = mockStatic(CqlIdentifier.class)) {
//                mockedStaticTwo.when(()->CqlIdentifier.fromCql(anyString())).thenReturn(identifier);
//                assertDoesNotThrow(()->batchUpdater.connect("", 0, "", ""));
//            }
//        }
//
//    }
//
//
//
//    @Test
//    public void testClose() {
//
//        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
//            mockedStatic.when(()->CqlSession.builder()).thenReturn(cqlSessionBuilder);
//            try (MockedStatic<CqlIdentifier> mockedStaticTwo = mockStatic(CqlIdentifier.class)) {
//                mockedStaticTwo.when(()->CqlIdentifier.fromCql(anyString())).thenReturn(identifier);
//                batchUpdater.connect("", 0, "", "");
//            }
//        }
//
//        assertDoesNotThrow(()->batchUpdater.close());
//
//    }
//
//
//    @Test
//    public void testGetSession() {
//
//        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
//            mockedStatic.when(()->CqlSession.builder()).thenReturn(cqlSessionBuilder);
//            try (MockedStatic<CqlIdentifier> mockedStaticTwo = mockStatic(CqlIdentifier.class)) {
//                mockedStaticTwo.when(()->CqlIdentifier.fromCql(anyString())).thenReturn(identifier);
//                batchUpdater.connect("", 0, "", "");
//            }
//        }
//
//        CqlSession session = batchUpdater.getSession();
//        assertNotNull(session);
//
//    }



}
