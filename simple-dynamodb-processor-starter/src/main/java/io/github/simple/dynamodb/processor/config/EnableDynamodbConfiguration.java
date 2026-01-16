package io.github.simple.dynamodb.processor.config;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Documented
@Import(value = {DynamodbConfig.class})
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableDynamodbConfiguration {

}
