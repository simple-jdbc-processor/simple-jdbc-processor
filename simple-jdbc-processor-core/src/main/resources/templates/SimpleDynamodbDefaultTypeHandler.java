package {{metadata.packageName}};

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;

import io.github.simple.dynamodb.processor.KeyPair;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@SuppressWarnings("unchecked")
public class {{metadata.typeHandlerClazzSimpleName}} {

{{#metadata.columnMetadataList}}

    public void decode{{firstUpFieldName}}(Map<String,AttributeValue> m, {{metadata.domainClazzName}} t, String column, Class<{{javaType}}> targetType){
        AttributeValue {{fieldName}} = m.get(column);
        if ({{fieldName}} != null) {
            t.set{{firstUpFieldName}}(({{javaType}}) getValue({{fieldName}}, targetType));
        }
    }

    public AttributeValue encode{{firstUpFieldName}}({{javaType}} value) {
        return getAttributeValue(value);
    }

{{/metadata.columnMetadataList}}

    public AttributeValue getAttributeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return AttributeValue.builder().s((String) value).build();
        }

        if (value instanceof BigDecimal) {
            BigDecimal atx = (BigDecimal) value;
            if (atx.compareTo(BigDecimal.ZERO) == 0) {
                return AttributeValue.builder().n("0").build();
            }
            return AttributeValue.builder().n(atx.stripTrailingZeros().toPlainString()).build();
        }

        if (value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        }
        if (value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean) value).build();
        }
        if (value instanceof byte[]) {
            return AttributeValue.builder().bs(SdkBytes.fromByteArray((byte[]) value)).build();
        }
        if (value instanceof Date) {
            return AttributeValue.builder().n((String.valueOf(((Date) value).getTime()))).build();
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) value;
            Instant instant = ldt.toInstant(ZoneOffset.UTC);
            return AttributeValue.builder().n(String.valueOf(instant.toEpochMilli())).build();
        }
        if (value instanceof LocalDate) {
            LocalDate ldt = (LocalDate) value;
            Instant instant = ldt.atStartOfDay(ZoneOffset.UTC).toInstant();
            return AttributeValue.builder().n(String.valueOf(instant.toEpochMilli())).build();
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return AttributeValue.builder().ss(Collections.emptyList()).build();
            }
            Object o = list.get(0);
            if (o instanceof String) {
                return AttributeValue.builder().ss((List<String>) value).build();
            }
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(baos);) {
            out.writeObject(value);
            out.flush();
            return AttributeValue.builder().b(SdkBytes.fromByteArray(baos.toByteArray())).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object getValue(AttributeValue av, Class<?> type) {
        if (av.ss() != null && !av.ss().isEmpty()) {
            return av.ss();
        }
        if (av.n() != null) {
            if (type == Long.class || type == long.class) {
                return Long.parseLong(av.n());
            }
            if (type == Integer.class || type == int.class) {
                return Integer.parseInt(av.n());
            }
            if (type == Double.class || type == double.class) {
                return Double.parseDouble(av.n());
            }
            if (type == Float.class || type == float.class) {
                return Float.parseFloat(av.n());
            }
            if (type == Boolean.class || type == boolean.class) {
                return Boolean.parseBoolean(av.n());
            }
            if (type == BigDecimal.class) {
                return new BigDecimal(av.n());
            }
            if (type == BigInteger.class) {
                return new BigInteger(av.n());
            }
            if (type == LocalDateTime.class) {
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(av.n())), ZoneOffset.UTC);
            }
            if (type == LocalDate.class) {
                return LocalDate.ofInstant(Instant.ofEpochMilli(Long.parseLong(av.n())), ZoneOffset.UTC);
            }
            if (type == Instant.class) {
                return Instant.ofEpochMilli(Long.parseLong(av.n()));
            }
            if (type == Date.class) {
                return new Date(Long.parseLong(av.n()));
            }
        }

        if (av.s() != null) {
            if (type == String.class) {
                return av.s();
            }
            if (type == StringBuilder.class) {
                return new StringBuilder(av.s());
            }

        }

        if (av.b() != null) {
            if (type == byte[].class) {
                return av.b().asByteArray();
            }
            if ((type.isAssignableFrom(Collection.class) || type.isAssignableFrom(Map.class))) {
                try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(av.b().asByteArray()))) {
                    return objectInputStream.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new IllegalArgumentException("Unknown AttributeValue type: " + av);
    }


    public {{metadata.domainClazzName}} decode(Map<String, AttributeValue> m) {
        if(m.isEmpty()){
            return null;
        }
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        {{#metadata.columnMetadataList}}
        decode{{firstUpFieldName}}(m,t,"{{originColumnName}}", {{javaType}}.class);
        {{/metadata.columnMetadataList}}
        return t;
    }

    public Map<String, AttributeValue> encode({{metadata.domainClazzName}} t) {
        Map<String, AttributeValue> m = new HashMap<>();
        {{#metadata.columnMetadataList}}
        if(t.get{{firstUpFieldName}}() != null){
            m.put("{{originColumnName}}", encode{{firstUpFieldName}}(t.get{{firstUpFieldName}}()));
        }
        {{/metadata.columnMetadataList}}
        return m;
    }

    public Map<String, AttributeValue> encode(KeyPair t) {
        Map<String, AttributeValue> m = new HashMap<>();
        {{#metadata.columnMetadataList}}
        {{#dynamodbHashKey}}
        if(t.getHashKeyAttributeValue() != null){
            m.put("{{originColumnName}}", t.getHashKeyAttributeValue());
        }
        {{/dynamodbHashKey}}
        {{#dynamodbRangeKey}}
        if(t.getRangeKeyAttributeValue() != null){
            m.put("{{originColumnName}}", t.getRangeKeyAttributeValue());
        }
        {{/dynamodbRangeKey}}
        {{/metadata.columnMetadataList}}
        return m;
    }

}
