package io.github.simple.jdbc.processor;

import io.github.simple.jdbc.processor.domain.DialectEnums;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface SimpleJdbc {


    /**
     * 使用spring上下文
     */
    boolean useSpring() default true;


    /**
     * 多数据库的情况下手动指定要使用主库.
     */
    String dataSource() default "";


    /**
     * 从库,如果配置该项,自动轮询.
     */
    String[] slaveDataSources() default {};


    /**
     * 方言.
     *
     * @return 方言.
     */
    DialectEnums dialect() default DialectEnums.NONE;


    /**
     * 分表
     */
    boolean shardTable() default false;

    /**
     * 是否继承 SimpleJdbcRepository 接口
     */
    boolean extendsSimpleJdbcRepository() default true;

    /**
     * 在没有@Column注解的情况下是否使用下划线,只针对jdbc数据库有效,MongoDB,ES等NoSQL数据库无效
     */
    boolean useUnderLine() default true;

    /**
     * 是否对字段关键词转义 password =>`password`,只针对jdbc数据库有效,MongoDB,ES等NoSQL数据库无效
     */
    boolean escape() default false;


}
