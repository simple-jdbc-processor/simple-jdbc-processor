package io.github.simple.jdbc.processor.domain;

public class ColumnMetadata {

    private String fieldName;

    private String columnName;

    private String originColumnName;

    private String firstUpFieldName;

    private String jdbcType;

    private String javaType;

    private boolean primary;

    private boolean useGeneratedKeys;

    private boolean stringType = false;

    private String typeHandler;

    private String javaDoc;

    private boolean updatable = true;

    private boolean insertable = true;

    private boolean version = false;

    private boolean isEnums = false;

    private String defaultValue = "null";

    private String fullJavaType;

    private boolean isCollection;

    public String getFieldName() {
        return fieldName;
    }

    public ColumnMetadata setFieldName(String fieldName) {
        this.fieldName = fieldName;
        if (fieldName != null) {
            if (fieldName.length() == 1) {
                this.firstUpFieldName = fieldName.toUpperCase();
            } else {
                this.firstUpFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            }
        }
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnMetadata setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getJdbcType() {
        return jdbcType;
    }

    public ColumnMetadata setJdbcType(String jdbcType) {
        this.jdbcType = jdbcType;
        return this;
    }

    public String getJavaType() {
        return javaType;
    }

    public ColumnMetadata setJavaType(String javaType) {
        String[] split = javaType.split("\\s+");
        this.javaType = split[split.length - 1];
        this.fullJavaType = javaType;
        int start = javaType.lastIndexOf("<");
        int end = javaType.indexOf(">");
        if (start != -1 && end != -1) {
            this.javaType = javaType.substring(start + 1, end);
        }
        this.stringType = "java.lang.String".equals(this.javaType);
        if (javaType.toLowerCase().contains("string")) {
            stringType = true;
        }
        if (javaType.equalsIgnoreCase("int") || javaType.equalsIgnoreCase("java.lang.Integer")) {
            this.javaType = "Integer";
            defaultValue = "0";
        }
        if (javaType.equalsIgnoreCase("long") || javaType.equalsIgnoreCase("java.lang.Long")) {
            this.javaType = "Long";
            this.defaultValue = "0L";
        }
        if (javaType.equalsIgnoreCase("short") || javaType.equalsIgnoreCase("java.lang.Short")) {
            this.javaType = "Short";
            this.defaultValue = "0";
        }
        if (javaType.equalsIgnoreCase("boolean") || javaType.equalsIgnoreCase("java.lang.Boolean")) {
            this.javaType = "Boolean";
        }
        if (fullJavaType.contains("java.util.List")
                || fullJavaType.contains("java.util.ArrayList")
                || fullJavaType.contains("java.util.Collection")
                || fullJavaType.contains("java.util.Set")
                || fullJavaType.contains("java.util.HashSet")
                || fullJavaType.contains("java.util.Map")
                || fullJavaType.contains("java.util.HashMap")) {
            setCollection(true);
        }
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public ColumnMetadata setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public boolean isUseGeneratedKeys() {
        return useGeneratedKeys;
    }

    public ColumnMetadata setUseGeneratedKeys(boolean useGeneratedKeys) {
        this.useGeneratedKeys = useGeneratedKeys;
        return this;
    }

    public String getFirstUpFieldName() {
        return firstUpFieldName;
    }

    public ColumnMetadata setFirstUpFieldName(String firstUpFieldName) {
        this.firstUpFieldName = firstUpFieldName;
        return this;
    }


    public boolean isStringType() {
        return stringType;
    }

    public void setStringType(boolean stringType) {
        this.stringType = stringType;
    }

    public String getTypeHandler() {
        return typeHandler;
    }

    public ColumnMetadata setTypeHandler(String typeHandler) {
        this.typeHandler = typeHandler;
        return this;
    }

    public String getJavaDoc() {
        return javaDoc;
    }

    public ColumnMetadata setJavaDoc(String javaDoc) {
        this.javaDoc = javaDoc;
        if (javaDoc != null && javaDoc.length() > 1) {
            this.javaDoc = javaDoc.substring(0, javaDoc.length() - 1);
        }
        return this;
    }

    public String getOriginColumnName() {
        return originColumnName;
    }

    public void setOriginColumnName(String originColumnName) {
        this.originColumnName = originColumnName;
    }

    public boolean isUpdatable() {
        return updatable;
    }

    public void setUpdatable(boolean updatable) {
        this.updatable = updatable;
    }

    public boolean isInsertable() {
        return insertable;
    }

    public void setInsertable(boolean insertable) {
        this.insertable = insertable;
    }

    public boolean isVersion() {
        return version;
    }

    public void setVersion(boolean version) {
        this.version = version;
    }

    public void setEnums(boolean enums) {
        this.isEnums = enums;
    }

    public boolean isEnums() {
        return isEnums;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setFullJavaType(String fullJavaType) {
        this.fullJavaType = fullJavaType;
    }

    public String getFullJavaType() {
        return fullJavaType;
    }

    public boolean isCollection() {
        return isCollection;
    }

    public void setCollection(boolean collection) {
        isCollection = collection;
    }
}
