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
    ORACLE(
            new DialectMetadata()
                    .setLeftEscape("\\\"")
                    .setRightEscape("\\\"")
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
