package com.smartshaped.fesr.framework.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class IncludeLookupTest {

    @Test
    void testLookup() {
        IncludeLookup includeLookup = new IncludeLookup("framework-config.yml");
        assertDoesNotThrow(() -> includeLookup.lookup("test"));
    }
}
