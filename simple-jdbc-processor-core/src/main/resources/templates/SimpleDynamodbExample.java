package {{metadata.packageName}};

import java.util.*;


@SuppressWarnings("unchecked")
public class {{metadata.exampleClazzSimpleName}} implements java.io.Serializable {

    private static final long serialVersionUID = 1000000L;

    private static final List<String> COLUMNS = new ArrayList<>();

    private Integer limit;

    private String orderByClause;

    private List<String> columns;

    private List<Criteria> criteries = new ArrayList<>();

    private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue > lastEvaluatedKey;

    private Boolean scanIndexForward = true;

    private String indexName;

    public {{metadata.exampleClazzSimpleName}}() {}

    public static {{metadata.exampleClazzSimpleName}} create(){
        return new {{metadata.exampleClazzSimpleName}}();
    }

    public {{metadata.exampleClazzSimpleName}} columns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} columns(String... columns) {
        this.columns = Arrays.asList(columns);
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} ignoreColumns(List<String> ignoreColumns) {
        this.columns = new ArrayList<>(COLUMNS);
        this.columns.removeAll(ignoreColumns);
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} ignoreColumns(String... ignoreColumns) {
        ignoreColumns(Arrays.asList(ignoreColumns));
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} desc() {
        this.scanIndexForward = false;
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} asc() {
        this.scanIndexForward = true;
        return this;
    }

    public Boolean getScanIndexForward() {
        return scanIndexForward;
    }

    public String getIndexName() {
        return indexName;
    }

    public {{metadata.exampleClazzSimpleName}} indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

{{#metadata.columnMetadataList}}

    public {{metadata.exampleClazzSimpleName}} and{{firstUpFieldName}}EqualTo({{fullJavaType}} value) {
        criteries.add(new Criteria("{{columnName}}", " = ", value));
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} and{{firstUpFieldName}}GreaterThan({{fullJavaType}} value) {
        criteries.add(new Criteria("{{columnName}}", " > ", value));
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} and{{firstUpFieldName}}GreaterThanOrEqualTo({{fullJavaType}} value) {
        criteries.add(new Criteria("{{columnName}}", " >= ", value));
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} and{{firstUpFieldName}}LessThan({{fullJavaType}} value) {
        criteries.add(new Criteria("{{columnName}}", " < ", value));
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} and{{firstUpFieldName}}LessThanOrEqualTo({{fullJavaType}} value) {
        criteries.add(new Criteria("{{columnName}}", " <= ", value));
        return this;
    }

{{/metadata.columnMetadataList}}


    public {{metadata.exampleClazzSimpleName}} limit(Integer size) {
        this.limit = size;
        return this;
    }

    public {{metadata.exampleClazzSimpleName}} setLastEvaluatedKey(Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> lastEvaluatedKey) {
        this.lastEvaluatedKey = lastEvaluatedKey;
        return this;
    }

    public List<Criteria> getCriteries() {
        return this.criteries;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Integer getLimit() {
        return limit;
    }

    public Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> getLastEvaluatedKey(){
        return lastEvaluatedKey;
    }

    public static interface Column {
    {{#metadata.columnMetadataList}}
        public static final String {{fieldName}} = "{{columnName}}";
    {{/metadata.columnMetadataList}}
    }


    public static interface Field {
    {{#metadata.columnMetadataList}}
        public static final String {{fieldName}} = "{{fieldName}}";
    {{/metadata.columnMetadataList}}
    }

    static {
        {{#metadata.columnMetadataList}}
        COLUMNS.add("{{columnName}}");
        {{/metadata.columnMetadataList}}
    }

    public static class Criteria {

        private String column;
        private String condition;

        private Object value;

        private Object secondValue;

        private List listValue;


        public Criteria(String column, String condition) {
            this.column = column;
            this.condition = condition;
        }

        public Criteria(String column, String condition, Object value) {
            this.column = column;
            this.condition = condition;
            this.value = value;
        }

        public Criteria(String column, String condition, Object value, Object secondValue) {
            this.column = column;
            this.condition = condition;
            this.value = value;
            this.secondValue = secondValue;
        }

        public Criteria(String column, String condition, List listValue) {
            this.column = column;
            this.condition = condition;
            this.listValue = listValue;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Object getSecondValue() {
            return secondValue;
        }

        public void setSecondValue(Object secondValue) {
            this.secondValue = secondValue;
        }

        public List getListValue() {
            return listValue;
        }

        public void setListValue(List<Object> listValue) {
            this.listValue = listValue;
        }
    }



}
