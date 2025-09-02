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

}
