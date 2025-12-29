package io.github.simple.jdbc.processor.domain;

public enum DialectEnums {
    NONE(
            new DialectMetadata()
    ),
    MYSQL(
            new DialectMetadata()
                    .setLeftEscape("`")
                    .setRightEscape("`")
    ),
    POSTGRES(
            new DialectMetadata()
                    .setLeftEscape("\\\"")
                    .setRightEscape("\\\"")
    ),
    MSSQL(
            new DialectMetadata()
                    .setLeftEscape("[")
                    .setRightEscape("]")
    ),
    ORACLE(POSTGRES.getValue()),
    DB2(POSTGRES.getValue()),
    DERBY(POSTGRES.getValue()),
    H2(POSTGRES.getValue()),
    SQLITE(POSTGRES.getValue()),
    REDSHIFT(POSTGRES.getValue()),
    GREENPLUM(POSTGRES.getValue()),
    CASSANDRA(POSTGRES.getValue()),
    DORIS(POSTGRES.getValue()),
    MONGO(new DialectMetadata()
            .setExampleJavaTemplatePath("templates/SimpleMongoExample.java")
            .setRepositoryTemplatePath("templates/SimpleMongoRepository.java")
            .setTypeHandlerTemplatePath("templates/SimpleMongoDefaultTypeHandler.java")
    ),
    ELASTICSEARCH(new DialectMetadata()
            .setExampleJavaTemplatePath("templates/SimpleElasticSearchExample.java")
            .setRepositoryTemplatePath("templates/SimpleElasticSearchRepository.java")
            .setTypeHandlerTemplatePath("templates/SimpleElasticSearchDefaultTypeHandler.java")
    ),
    ELASTICSEARCH_V9(new DialectMetadata()
            .setExampleJavaTemplatePath("templates/SimpleElasticSearchExampleV9.java")
            .setRepositoryTemplatePath("templates/SimpleElasticSearchRepositoryV9.java")
            .setTypeHandlerTemplatePath("templates/SimpleElasticSearchDefaultTypeHandlerV9.java")
    ),
    ;

    private final DialectMetadata value;

    DialectEnums(DialectMetadata value) {
        this.value = value;
    }

    public DialectMetadata getValue() {
        return value;
    }
}
