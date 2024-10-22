package com.smartshaped.fesr.framework.common.utils;

import com.smartshaped.fesr.framework.common.exception.ConfigurationException;

public class ConfigurationUtilsExample extends ConfigurationUtils {

    protected ConfigurationUtilsExample() throws ConfigurationException {
        super();

        setConfRoot("test.");
    }
}
