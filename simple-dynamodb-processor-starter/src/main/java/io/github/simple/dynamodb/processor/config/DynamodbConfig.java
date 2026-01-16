package io.github.simple.dynamodb.processor.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

@EnableConfigurationProperties(DynamodbProperties.class)
public class DynamodbConfig {


    @ConditionalOnMissingBean
    @Bean
    public DynamoDbClient dynamoDbClient(DynamodbProperties dynamodbProperties) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder();
        if (dynamodbProperties.getEndpoint() != null && !dynamodbProperties.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(dynamodbProperties.getEndpoint()));
        }
        if (dynamodbProperties.getAccessKey() != null && dynamodbProperties.getSecretKey() != null) {
            builder.credentialsProvider(() -> AwsBasicCredentials.create(dynamodbProperties.getAccessKey(), dynamodbProperties.getSecretKey()));
        }
        if (dynamodbProperties.getRegion() != null) {
            builder.region(dynamodbProperties.getRegion());
        }
        return builder.build();
    }
}
