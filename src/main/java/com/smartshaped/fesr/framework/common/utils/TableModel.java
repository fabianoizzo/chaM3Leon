package com.smartshaped.fesr.framework.common.utils;

import com.smartshaped.fesr.framework.common.exception.CassandraException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is the base class for all classes that needs to interact with
 * Cassandra database.
 */
public abstract class TableModel {

    private String tableName;
    private String primaryKey;
    private boolean multipleKeys = false;
    private boolean generateUuid = false;


    /**
     * Validates the model by checking if the primary key is valid.
     * <p>
     * This method will throw a {@link CassandraException} if the model is not valid.
     *
     * @throws CassandraException if the model is not valid
     */
    public void validateModel() throws CassandraException {

        primaryKey = choosePrimaryKey();
        tableName = getTableName();

        Field[] fields = this.getClass().getDeclaredFields();

        checkPrimaryKey(fields);
    }

    /**
     * Return a Cassandra query to create the table for this class.
     *
     * @return a Cassandra query to create the table, parametrized with the keyspace name
     */
    public String getCreationQuery() {

        String fieldsSchema = getSchema();

        return "CREATE TABLE IF NOT EXISTS ?." + tableName + " (" + fieldsSchema + ");";
    }

    /**
     * Checks if the primary key is valid.
     * <p>
     * If the primary key is empty, a UUID is generated as the primary key.
     * If the primary key is not empty, it checks if there are multiple keys
     * and if the primary key is valid.
     *
     * @param fields the fields of the class
     * @throws CassandraException if the primary key is not valid
     */
    private void checkPrimaryKey(Field[] fields) throws CassandraException {

        primaryKey = choosePrimaryKey();

        // if primary key is empty, we generate a uuid
        if (primaryKey.trim().isEmpty()) {

            generateUuid = true;
        } else {
            // check if there are multiple keys
            String[] primaryKeys = primaryKey.replaceAll(" ", "").split(",");

            if (primaryKeys.length > 1) {

                multipleKeys = true;
            }

            checkValidPrimaryKey(fields, primaryKeys);
        }
    }

    /**
     * Checks if all the given primary keys are valid for the given class.
     * <p>
     * This method will check if all the given primary keys are valid for the given class.
     * If any of the primary keys are not valid, a {@link CassandraException} is thrown.
     *
     * @param fields      the fields of the class
     * @param primaryKeys the primary keys to check
     * @throws CassandraException if any of the primary keys are not valid
     */
    private void checkValidPrimaryKey(Field[] fields, String[] primaryKeys) throws CassandraException {

        for (String x : primaryKeys) {
            checkValidPrimaryKey(fields, x);
        }
    }

    /**
     * Checks if a given primary key is valid for the given class.
     * <p>
     * This method will check if the given primary key is valid for the given class.
     * If the primary key is not valid, a {@link CassandraException} is thrown.
     *
     * @param fields the fields of the class
     * @param x      the primary key to check
     * @throws CassandraException if the primary key is not valid
     */
    private void checkValidPrimaryKey(Field[] fields, String x) throws CassandraException {

        List<String> fieldsList = new ArrayList<>();
        for (Field field : fields) {
            fieldsList.add(field.getName());
        }
        if (!fieldsList.contains(x)) {

            throw new CassandraException("Primary key " + x + " not found");
        }
    }

    /**
     * Builds a Cassandra schema string based on the fields of this class.
     *
     * <p>This method will generate a Cassandra schema string that can be used to create a table.
     * The primary key is set to the fields specified in the {@link #primaryKey} field.
     * If the primary key is empty, a uuid is generated.
     * If the primary key is a single field, it is used as the primary key.
     * If the primary key is a list of fields, they are used as the primary key.
     *
     * @return a Cassandra schema string
     */
    private String getSchema() {

        StringBuilder fieldsSchema = new StringBuilder();

        Field field;
        String fieldName = "";
        String fieldType = "";

        if (generateUuid) {
            fieldsSchema.append("id uuid PRIMARY KEY, ");
        }

        Field[] fields = this.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            field = fields[i];

            fieldName = field.getName();

            fieldType = field.getType().getSimpleName().toLowerCase();
            fieldType = convertFieldType(fieldType);
            fieldsSchema.append(fieldName).append(" ").append(fieldType);

            if (primaryKey.equals(fieldName)) {
                fieldsSchema.append(" PRIMARY KEY");
            }

            if (i < fields.length - 1) {
                fieldsSchema.append(", ");
            }
        }

        if (multipleKeys) {
            fieldsSchema.append(", PRIMARY KEY (").append(primaryKey).append(")");
        }

        return fieldsSchema.toString();
    }

    /**
     * Converts a Java type to a Cassandra type.
     *
     * @param fieldType the Java type
     * @return the corresponding Cassandra type
     */
    private String convertFieldType(String fieldType) {
        if (fieldType.equals("string")) {
            return "text";
        }
        return fieldType;
    }

    public String getTableName() {
        return this.getClass().getSimpleName().toLowerCase();
    }

    public boolean isGenerateUuid() {
        return generateUuid;
    }

    protected abstract String choosePrimaryKey();
}
