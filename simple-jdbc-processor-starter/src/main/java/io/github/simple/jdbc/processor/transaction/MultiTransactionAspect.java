package io.github.simple.jdbc.processor.transaction;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 切面：处理带有 @MultiTransactional 注解的方法，支持多个事务管理器
 */
@Aspect
public class MultiTransactionAspect {

    /**
     * 注入 Spring 容器中所有的事务管理器（key：事务管理器 ID，value：事务管理器实例）
     */
    private final Map<String, PlatformTransactionManager> txManagerMap;

    /**
     * 构造器注入所有事务管理器
     *
     * @param txManagerMap 事务管理器映射，key 为事务管理器 ID，value 为事务管理器实例
     */
    public MultiTransactionAspect(Map<String, PlatformTransactionManager> txManagerMap) {
        this.txManagerMap = txManagerMap;
    }

    /**
     * 切面：拦截带有 @MultiTransactional 注解的方法
     *
     * @param joinPoint          目标方法的连接点
     * @param multiTransactional 注解实例，包含事务管理器 ID、回滚规则等
     * @return 目标方法的返回值
     * @throws Throwable 目标方法抛出的异常
     */
    @Around("@annotation(multiTransactional)")
    public Object around(ProceedingJoinPoint joinPoint, MultiTransactional multiTransactional) throws Throwable {
        String[] txManagerIds = multiTransactional.transactionManagers();
        Class<? extends Throwable>[] rollbackFor = multiTransactional.rollbackFor();
        Class<? extends Throwable>[] noRollbackFor = multiTransactional.noRollbackFor();

        // 存储每个事务的状态（用于异常时回滚）
        List<TransactionStatus> txStatusList = new ArrayList<>();

        try {
            // 1. 初始化所有事务（按注解指定的事务管理器顺序）
            for (String txManagerId : txManagerIds) {
                PlatformTransactionManager txManager = txManagerMap.get(txManagerId);
                if (txManager == null) {
                    throw new IllegalArgumentException("事务管理器不存在：" + txManagerId);
                }
                // 配置事务属性
                DefaultTransactionDefinition txDefinition = new DefaultTransactionDefinition();
                txDefinition.setIsolationLevel(multiTransactional.isolation().value());
                txDefinition.setPropagationBehavior(multiTransactional.propagation().value());
                txDefinition.setTimeout(multiTransactional.timeout());
                txDefinition.setReadOnly(multiTransactional.readOnly());

                // 开启事务，记录状态
                TransactionStatus txStatus = txManager.getTransaction(txDefinition);
                txStatusList.add(txStatus);
            }

            // 2. 执行目标方法（业务逻辑）
            Object result = joinPoint.proceed();

            // 3. 所有操作成功，提交所有事务
            for (int i = 0; i < txManagerIds.length; i++) {
                PlatformTransactionManager txManager = txManagerMap.get(txManagerIds[i]);
                txManager.commit(txStatusList.get(i));
            }

            return result;
        } catch (Throwable e) {
            boolean isNeedRollback = isNeedRollback(e, rollbackFor, noRollbackFor);
            if (isNeedRollback) {
                // 4. 发生异常，回滚所有事务
                for (int i = 0; i < txManagerIds.length; i++) {
                    PlatformTransactionManager txManager = txManagerMap.get(txManagerIds[i]);
                    TransactionStatus txStatus = txStatusList.get(i);
                    if (!txStatus.isCompleted()) { // 未提交的事务才回滚
                        txManager.rollback(txStatus);
                    }
                }
            }
            throw e;
        }
    }


    /**
     * 核心工具方法：判断异常是否需要回滚（与 Spring 原生规则一致）
     *
     * @param e             发生的异常
     * @param rollbackFor   需回滚的异常数组
     * @param noRollbackFor 不回滚的异常数组
     * @return 是否需要回滚
     */
    private boolean isNeedRollback(Throwable e, Class<? extends Throwable>[] rollbackFor, Class<? extends Throwable>[] noRollbackFor) {
        // 步骤 1：判断是否在 noRollbackFor 中（优先级最高）
        if (noRollbackFor != null) {
            for (Class<? extends Throwable> clazz : noRollbackFor) {
                if (clazz.isInstance(e)) {
                    return false;
                }
            }
        }

        // 步骤 2：判断是否在 rollbackFor 中
        if (rollbackFor != null && rollbackFor.length > 0) {
            for (Class<? extends Throwable> clazz : rollbackFor) {
                if (clazz.isInstance(e)) {
                    return true;
                }
            }
            return false;
        }

        // 步骤 3：默认规则（RuntimeException 或 Error 回滚）
        return e instanceof RuntimeException || e instanceof Error;
    }

}