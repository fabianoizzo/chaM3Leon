package com.smartshaped.fesr.framework.common.utils;

public class TableModelExample extends TableModel {

    String test1;
    int test2;

    @Override
    protected String choosePrimaryKey() {
        return "";
    }
}
