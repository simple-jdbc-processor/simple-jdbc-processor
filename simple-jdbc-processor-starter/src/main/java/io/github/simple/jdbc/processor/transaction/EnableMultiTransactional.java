package io.github.simple.jdbc.processor.transaction;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Documented
@Import(value = {MultiTransactionAspect.class})
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableMultiTransactional {

}
