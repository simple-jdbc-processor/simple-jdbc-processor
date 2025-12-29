package io.github.simple.jdbc.processor.transaction;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MultiTransactional {

    /**
     * 事务管理器 ID 数组，指定要使用的多个事务管理器
     */
    String[] transactionManagers();

    /**
     * 需回滚的异常数组，指定哪些异常触发回滚
     */
    Class<? extends Throwable>[] rollbackFor() default Exception.class;

    /**
     * 不回滚的异常数组，指定哪些异常不触发回滚
     */
    Class<? extends Throwable>[] noRollbackFor() default {}; // 不回滚的异常

    /**
     * 事务传播行为，默认值为 Propagation.REQUIRED
     */
    Propagation propagation() default Propagation.REQUIRED;


    /**
     * 事务隔离级别，默认值为 Isolation.DEFAULT
     */
    Isolation isolation() default Isolation.DEFAULT;

    /**
     * 事务超时时间，默认值为 TransactionDefinition.TIMEOUT_DEFAULT
     */
    int timeout() default TransactionDefinition.TIMEOUT_DEFAULT;


    /**
     * 是否为只读事务，默认值为 false
     */
    boolean readOnly() default false;

}