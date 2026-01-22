package io.github.simple.jdbc.processor;

import io.github.simple.jdbc.processor.domain.DialectEnums;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface SimpleJdbc {

    /**
     * 多数据库的情况下手动指定要使用主库.
     */
    String dataSource() default "";

    /**
     * 指定表名优先级最高, or @Table 注解.
     */
    String tableName() default "";

    /**
     * 指定主键, or @Id 注解.
     */
    String tablePrimaryKey() default "";

    /**
     * 从库,如果配置该项,自动轮询.
     */
    String[] slaveDataSources() default {};

    /**
     * 是否只读,默认false.
     */
    boolean readOnly() default false;

    /**
     * 方言.
     *
     * @return 方言.
     */
    DialectEnums dialect() default DialectEnums.NONE;


    /**
     * 分表
     *
     * @return 是否分表
     */
    boolean shardTable() default false;

    /**
     * 在没有@Column注解的情况下是否使用下划线,只针对jdbc数据库有效,MongoDB,ES等NoSQL数据库无效
     *
     * @return 是否在没有@Column注解的情况下是否使用下划线
     */
    boolean useUnderLine() default true;

    /**
     * 是否对字段关键词转义 password 转`password`,只针对jdbc数据库有效,MongoDB,ES等NoSQL数据库无效
     *
     * @return 是否对字段关键词转义
     */
    boolean escape() default false;

    /**
     * 是否审计sql
     *
     * @return 是否审计sql
     */
    boolean auditSql() default false;

}
