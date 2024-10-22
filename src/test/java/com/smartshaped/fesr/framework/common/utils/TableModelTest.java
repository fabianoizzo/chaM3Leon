package com.smartshaped.fesr.framework.common.utils;

import com.smartshaped.fesr.framework.common.exception.CassandraException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableModelTest {

    TableModel tableModel;

    @Test
    void testNoPK() throws CassandraException {

        tableModel = new TableModelExample();
        tableModel.validateModel();

        assertDoesNotThrow(() -> tableModel.getCreationQuery());
    }

    @Test
    void testSinglePK() throws CassandraException {

        tableModel = new TableModelExample1();
        tableModel.validateModel();

        assertDoesNotThrow(() -> tableModel.getCreationQuery());
    }

    @Test
    void testMultiplePK() throws CassandraException {

        tableModel = new TableModelExample2();
        tableModel.validateModel();

        assertDoesNotThrow(() -> tableModel.getCreationQuery());
    }

    @Test
    void testWrongPK() {

        tableModel = new TableModelExample3();

        assertThrows(CassandraException.class, () -> tableModel.validateModel());
    }
}
