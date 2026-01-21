package {{metadata.packageName}};

import io.github.simple.jdbc.processor.SimpleJdbcRepository;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;

import io.github.simple.dynamodb.processor.KeyPair;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class {{metadata.repositoryClazzSimpleName}} implements io.github.simple.jdbc.processor.SimpleJdbcRepository<{{metadata.domainClazzSimpleName}}, KeyPair, {{metadata.exampleClazzSimpleName}}> {

    private final String tableName = "{{metadata.tableName}}";

    protected DynamoDbClient dynamoDbClient;

    private {{metadata.typeHandlerClazzName}} defaultTypeHandler = new {{metadata.typeHandlerClazzName}}();


    @Override
    public {{metadata.domainClazzName}} selectByPrimaryKey(KeyPair keyPair) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(defaultTypeHandler.encode(keyPair))
                .build();
        GetItemResponse response = dynamoDbClient.getItem(request);
        return defaultTypeHandler.decode(response.item());
    }

    @Override
    public {{metadata.domainClazzName}} selectByPrimaryKeyForUpdate(KeyPair keyPair) {
        return selectByPrimaryKey(keyPair);
    }

    @Override
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeys(List<KeyPair> ts) {
        KeysAndAttributes keysAndAttributes = KeysAndAttributes.builder()
                .keys(ts.stream().map(defaultTypeHandler::encode).collect(Collectors.toList()))
                .build();
        Map<String,KeysAndAttributes> keysAndAttributesMap = new HashMap<>();
        keysAndAttributesMap.put(tableName,keysAndAttributes);
        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(keysAndAttributesMap)
                .build();
        BatchGetItemResponse response = dynamoDbClient.batchGetItem(request);
        List<{{metadata.domainClazzName}}> result = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, AttributeValue>>> entry : response.responses().entrySet()) {
            List<Map<String, AttributeValue>> values = entry.getValue();
            for (Map<String, AttributeValue> value : values) {
                {{metadata.domainClazzName}} p = defaultTypeHandler.decode(value);
                result.add(p);
            }
        }
        return result;
    }

    @Override
    public List<{{metadata.domainClazzName}}> selectByExample({{metadata.exampleClazzName}} query) {
        QueryRequest queryRequest = toQueryRequest(query);
        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        query.setLastEvaluatedKey(lastEvaluatedKey);
        return queryResponse.items().stream().map(defaultTypeHandler::decode).collect(Collectors.toList());
    }

    @Override
    public void consumeByExample({{metadata.exampleClazzName}} query, Consumer<{{metadata.domainClazzName}}> consumer) {
        QueryRequest queryRequest = toQueryRequest(query);
        QueryIterable queryResponse = dynamoDbClient.queryPaginator(queryRequest);
        for (Map<String, AttributeValue> item : queryResponse.items()) {
            {{metadata.domainClazzName}} p = defaultTypeHandler.decode(item);
            consumer.accept(p);
        }
    }

    @Override
    public {{metadata.domainClazzName}} selectOne({{metadata.exampleClazzName}} query) {
        List<{{metadata.domainClazzName}}> ts = selectByExample(query);
        return ts.isEmpty() ? null : ts.get(0);
    }

    @Override
    public List<{{metadata.domainClazzName}}> selectAll() {
        QueryRequest queryRequest = QueryRequest.builder().build();
        QueryIterable queryResponse = dynamoDbClient.queryPaginator(queryRequest);
        return queryResponse.items().stream().map(defaultTypeHandler::decode).collect(Collectors.toList());
    }

    @Override
    public void insert({{metadata.domainClazzName}} t) {
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(defaultTypeHandler.encode(t))
                .build();
        dynamoDbClient.putItem(request);
    }

    @Override
    public void insertSelective({{metadata.domainClazzName}} t) {
        insert(t);
    }

    @Override
    public void upsertSelective({{metadata.domainClazzName}} t) {
        insert(t);
    }

    @Override
    public void insertBatch(List<{{metadata.domainClazzName}}> ts) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = requestItems.computeIfAbsent(tableName, k -> new ArrayList<>());

        for ({{metadata.domainClazzName}} t : ts) {
            WriteRequest writeRequest = WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(defaultTypeHandler.encode(t)).build())
                    .build();
            writeRequests.add(writeRequest);
        }

        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
        dynamoDbClient.batchWriteItem(request);
    }

    @Override
    public void insertIgnoreBatch(List<{{metadata.domainClazzName}}> ts) {
        insertBatch(ts);
    }

    @Override
    public long countByExample({{metadata.exampleClazzName}} query) {
        throw new UnsupportedOperationException("countByExample is not supported");
    }

    @Override
    public int updateByPrimaryKeySelective({{metadata.domainClazzName}} t) {
        insert(t);
        return 1;
    }

    @Override
    public int updateByPrimaryKey({{metadata.domainClazzName}} t) {
        insert(t);
        return 1;
    }

    @Override
    public int updateByExampleSelective({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} query) {
        throw new UnsupportedOperationException("updateByExampleSelective is not supported");
    }

    @Override
    public int updateByExample({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} query) {
        throw new UnsupportedOperationException("updateByExample is not supported");
    }

    @Override
    public int deleteByPrimaryKey(KeyPair keyPair) {
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(defaultTypeHandler.encode(keyPair))
                .build();
        dynamoDbClient.deleteItem(request);
        return 1;
    }

    @Override
    public int deleteByPrimaryKeys(List<KeyPair> keyPairs) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = requestItems.computeIfAbsent(tableName, k -> new ArrayList<>());

        for (KeyPair keyPair : keyPairs) {
            WriteRequest writeRequest = WriteRequest.builder()
                    .deleteRequest(DeleteRequest.builder().key(defaultTypeHandler.encode(keyPair)).build())
                    .build();
            writeRequests.add(writeRequest);
        }

        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
        BatchWriteItemResponse batchWriteItemResponse = dynamoDbClient.batchWriteItem(request);
        return keyPairs.size() - batchWriteItemResponse.unprocessedItems().size();
    }

    @Override
    public int deleteByExample({{metadata.exampleClazzName}} query) {
        throw new UnsupportedOperationException("deleteByExample is not supported");
    }

    @Override
    public boolean existsById(KeyPair keyPair) {
        return selectByPrimaryKey(keyPair) != null;
    }

    @Override
    public boolean existsByExample({{metadata.exampleClazzName}} query) {
        return !selectByExample(query).isEmpty();
    }

    @Override
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeysWithSorted(List<KeyPair> keyPairs) {
        return selectByPrimaryKeys(keyPairs);
    }

    @Override
    public Map<KeyPair, {{metadata.domainClazzName}}> mapById(List<KeyPair> keyPairs) {
        Map<KeyPair, {{metadata.domainClazzName}}> result = new HashMap<>();
        for ({{metadata.domainClazzName}} t : selectByPrimaryKeys(keyPairs)) {
            KeyPair keyPair = new KeyPair();

            {{#metadata.columnMetadataList}}
            {{#dynamodbHashKey}}
            if({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}){
                keyPair.setHashKey(t.get{{firstUpFieldName}}());
            }
            {{/dynamodbHashKey}}
            {{#dynamodbRangeKey}}
            if({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}){
                keyPair.setRangeKey(t.get{{firstUpFieldName}}());
            }
            {{/dynamodbRangeKey}}
            {{/metadata.columnMetadataList}}
            result.put(keyPair, t);
        }
        return result;
    }

    public QueryRequest toQueryRequest({{metadata.exampleClazzName}} query) {
        QueryRequest.Builder builder = QueryRequest.builder()
                .tableName(tableName);
        if (query.getColumns() != null) {
            builder.projectionExpression(String.join(",", query.getColumns()));
        }
        if (query.getLimit() != null &&query.getLimit()>0) {
            builder.limit(query.getLimit());
        }
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        List<String> keyExpressions = new ArrayList<>();
        List<String> filterExpressions = new ArrayList<>();
        for ({{metadata.exampleClazzName}}.Criteria critery : query.getCriteries()) {
            String condition = critery.getCondition();
            String column = critery.getColumn();
            Object value = critery.getValue();
            List listValue = critery.getListValue();
            String varName = ":" + column;
            String expression = column + condition + varName;


            {{#metadata.columnMetadataList}}
            if ("{{originColumnName}}".equals(column) || "{{columnName}}".equals(column) || "{{fieldName}}".equals(column)) {
                AttributeValue attributeValue = defaultTypeHandler.encode{{firstUpFieldName}}(({{fullJavaType}}) value);
                expressionAttributeValues.put(varName, attributeValue);
                if({{{dynamodbHashKey}}}){
                    keyExpressions.add(expression);
                } else if({{{dynamodbRangeKey}}}){
                    keyExpressions.add(expression);
                } else {
                    filterExpressions.add(expression);
                }
            }
            {{/metadata.columnMetadataList}}
        }

        builder.expressionAttributeValues(expressionAttributeValues);
        if (!keyExpressions.isEmpty()) {
            builder.keyConditionExpression(String.join(" and ", keyExpressions));
        }
        if (!filterExpressions.isEmpty()) {
            builder.filterExpression(String.join(" and ", filterExpressions));
        }

        if(query.getLastEvaluatedKey() !=null && !query.getLastEvaluatedKey().isEmpty()){
            builder.exclusiveStartKey(query.getLastEvaluatedKey());
        }

        if(query.getScanIndexForward() != null){
            builder.scanIndexForward(query.getScanIndexForward());
        }

        if(query.getIndexName() != null && !query.getIndexName().isEmpty()){
            builder.indexName(query.getIndexName());
        }
        return builder.build();
    }

{{#metadata.dataSource}}@org.springframework.beans.factory.annotation.Qualifier("{{metadata.dataSource}}"){{/metadata.dataSource}}
    @Autowired
    public void setDynamoDbClient(DynamoDbClient dynamoDbClient){
        this.dynamoDbClient = dynamoDbClient;
    }

    @Autowired(required = false)
    public void setDefaultTypeHandler({{metadata.typeHandlerClazzName}} defaultTypeHandler){
        this.defaultTypeHandler = defaultTypeHandler;
    }
}
