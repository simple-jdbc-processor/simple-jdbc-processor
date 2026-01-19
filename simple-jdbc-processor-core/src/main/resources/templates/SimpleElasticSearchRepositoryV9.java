package {{metadata.packageName}};


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.NumberRangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import io.github.simple.jdbc.processor.SimpleJdbcRepository;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;


@SuppressWarnings("unchecked")
public class {{metadata.repositoryClazzSimpleName}} {{#metadata.extendsSimpleJdbcRepository}}implements io.github.simple.jdbc.processor.SimpleJdbcRepository<{{metadata.domainClazzSimpleName}}, {{metadata.primaryMetadata.javaType}}, {{metadata.exampleClazzSimpleName}}> {{/metadata.extendsSimpleJdbcRepository}} {

    private String indexName = "{{metadata.tableName}}";

    protected ElasticsearchClient elasticsearchClient;


    
    public {{metadata.domainClazzName}} selectByPrimaryKey({{metadata.primaryMetadata.javaType}} id) {
        GetRequest request = GetRequest.of(g -> g.index(indexName)
                .id(String.valueOf(id))
        );
        try {
            GetResponse<{{metadata.domainClazzName}}> response = elasticsearchClient.get(request, {{metadata.domainClazzName}}.class);
            return response.found() ? response.source() : null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public {{metadata.domainClazzName}} selectByPrimaryKeyForUpdate({{metadata.primaryMetadata.javaType}} id) {
        return selectByPrimaryKey(id);
    }

    
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> ids) {
        MgetRequest get = MgetRequest.of(
                g -> g.index(indexName)
                        .ids(ids.stream().map(String::valueOf).toList())
        );
        List<{{metadata.domainClazzName}}> ts = new ArrayList<>(ids.size());
        try {
            MgetResponse<{{metadata.domainClazzName}}> response = elasticsearchClient.mget(get, {{metadata.domainClazzName}}.class);
            List<MultiGetResponseItem<{{metadata.domainClazzName}}>> docs = response.docs();
            for (MultiGetResponseItem<{{metadata.domainClazzName}}> doc : docs) {
                GetResult<{{metadata.domainClazzName}}> result = doc.result();
                if (result.found()) {
                    ts.add(result.source());
                }
            }
            return ts;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public List<{{metadata.domainClazzName}}> selectByExample({{metadata.exampleClazzName}} example) {
        SearchRequest request = SearchRequest.of(s -> () -> {
                    SearchRequest.Builder r = new SearchRequest.Builder()
                            .index(indexName);
                    appendLimitSort(r, example);
                    ObjectBuilder<Query> query = buildQueryFromExample(new Query.Builder(), example);
                    return new SearchRequest.Builder()
                            .query(q -> query)
                            .build();
                }
        );

        try {
            SearchResponse<{{metadata.domainClazzName}}> response = elasticsearchClient.search(request, {{metadata.domainClazzName}}.class);
            return response.hits().hits().stream()
                    .map(Hit::source)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void consumeByExample({{metadata.exampleClazzName}} example, Consumer<{{metadata.domainClazzName}}> consumer) {
        ObjectBuilder<Query> query = buildQueryFromExample(new Query.Builder(), example);
        String scrollId;
        SearchRequest request = SearchRequest.of(s -> () -> {
                    SearchRequest.Builder r = new SearchRequest.Builder()
                            .index(indexName)
                            .scroll(Time.of(f -> f.time("30m")));
                    appendLimitSort(r, example);
                    return new SearchRequest.Builder()
                            .query(q -> query)
                            .build();
                }
        );

        try {
            SearchResponse<{{metadata.domainClazzName}}> response = elasticsearchClient.search(request, {{metadata.domainClazzName}}.class);
            scrollId = response.scrollId();
            List<Hit<{{metadata.domainClazzName}}>> hits = response.hits().hits();
            if (hits.isEmpty()) {
                return;
            }
            hits.forEach(s -> consumer.accept(s.source()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        while (scrollId != null) {
            try {
                String copyScrollId = scrollId;
                ScrollResponse<{{metadata.domainClazzName}}> scrollResponse = elasticsearchClient.scroll(ScrollRequest.of(f ->
                                f.scroll(Time.of(t -> t.time("10m"))).scrollId(copyScrollId)),
                        {{metadata.domainClazzName}}.class);
                List<Hit<{{metadata.domainClazzName}}>> hits = scrollResponse.hits().hits();
                hits.forEach(s -> consumer.accept(s.source()));
                if (hits.isEmpty()) {
                    elasticsearchClient.clearScroll(c -> c
                            .scrollId(copyScrollId)
                    );
                    return;
                }
                scrollId = scrollResponse.scrollId();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }


    
    public {{metadata.domainClazzName}} selectOne({{metadata.exampleClazzName}} query) {
        List<{{metadata.domainClazzName}}> results = selectByExample(query);
        return results.isEmpty() ? null : results.get(0);
    }

    
    public List<{{metadata.domainClazzName}}> selectAll() {
        List<{{metadata.domainClazzName}}> results = new ArrayList<>();
        consumeByExample(new {{metadata.exampleClazzName}}(), results::add);
        return results;
    }

    
    public void insert({{metadata.domainClazzName}} t) {
        insertSelective(t);
    }

    
    public void insertSelective({{metadata.domainClazzName}} t) {
        // 选择性插入（忽略null字段），需要开启部分更新模式
        IndexRequest<{{metadata.domainClazzName}}> request = IndexRequest.of(i -> i
                .index(indexName)
                .id(String.valueOf(t.get{{metadata.primaryMetadata.firstUpFieldName}}()))
                .document(t)
        );

        try {
            elasticsearchClient.index(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public void upsertSelective({{metadata.domainClazzName}} t) {
        // 更新或插入（存在则更新，不存在则插入）
        IndexRequest<{{metadata.domainClazzName}}> request = IndexRequest.of(i -> i
                .index(indexName)
                .id(String.valueOf(t.get{{metadata.primaryMetadata.firstUpFieldName}}()))
                .document(t)
                .opType(OpType.Index)
        );

        try {
            elasticsearchClient.index(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertIgnoreBatch(List<{{metadata.domainClazzName}}> ts){
        insertBatch(ts);
    }
    
    public void insertBatch(List<{{metadata.domainClazzName}}> ts) {
        List<BulkOperation> operations = ts.stream()
                .map(t -> BulkOperation.of(b -> b
                        .index(IndexOperation.of(i -> i
                                .index(indexName)
                                .id(String.valueOf(t.get{{metadata.primaryMetadata.firstUpFieldName}}()))
                                .document(t)
                        ))
                ))
                .collect(Collectors.toList());

        BulkRequest request = BulkRequest.of(b -> b
                .index(indexName)
                .operations(operations)
        );

        try {
            BulkResponse response = elasticsearchClient.bulk(request);
            if (response.errors()) {
                throw new RuntimeException("Bulk operations failed");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public long countByExample({{metadata.exampleClazzName}} query) {
        CountRequest request = CountRequest.of(c -> c
                .index(indexName)
                .query(q -> buildQueryFromExample(q, query))
        );

        try {
            CountResponse response = elasticsearchClient.count(request);
            return response.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public int updateByPrimaryKeySelective({{metadata.domainClazzName}} t) {
        UpdateRequest<{{metadata.domainClazzName}}, {{metadata.domainClazzName}}> request = UpdateRequest.of(u -> u
                .index(indexName)
                .id(String.valueOf(t.get{{metadata.primaryMetadata.firstUpFieldName}}()))
                .doc(t)
                .docAsUpsert(false)
                .retryOnConflict(3)
        );

        try {
            UpdateResponse<{{metadata.domainClazzName}}> response = elasticsearchClient.update(request, {{metadata.domainClazzName}}.class);
            return response.result() == Result.Updated ? 1 : 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public int updateByPrimaryKey({{metadata.domainClazzName}} t) {
        return updateByPrimaryKeySelective(t);
    }

    
    public int updateByExampleSelective({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} query) {
        // 根据条件选择性更新
        UpdateByQueryRequest request = UpdateByQueryRequest.of(u -> u
                .index(indexName)
                .query(q -> buildQueryFromExample(q, query))
                .script(s -> s.source(f -> f.scriptString(buildUpdateScript(t))).params(buildUpdateScriptParams(t)))
        );

        try {
            UpdateByQueryResponse response = elasticsearchClient.updateByQuery(request);
            return response.updated() == null ? 0 : response.updated().intValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public int updateByExample({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} query) {
        // 同选择性更新，如需强制更新null字段需调整脚本
        return updateByExampleSelective(t, query);
    }

    
    public int deleteByPrimaryKey({{metadata.primaryMetadata.javaType}} id) {
        DeleteRequest request = DeleteRequest.of(d -> d
                .index(indexName)
                .id(String.valueOf(id))
        );

        try {
            DeleteResponse response = elasticsearchClient.delete(request);
            return response.result() == Result.Deleted ? 1 : 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public int deleteByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> ids) {
        BulkRequest request = BulkRequest.of(b -> b
                .index(indexName)
                .operations(ids.stream()
                        .map(id -> BulkOperation.of(bo -> bo
                                .delete(d -> d.id(String.valueOf(id)))
                        ))
                        .collect(Collectors.toList())
                )
        );

        try {
            BulkResponse response = elasticsearchClient.bulk(request);
            return response.errors() ? 0 : response.items().size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public int deleteByExample({{metadata.exampleClazzName}} query) {
        DeleteByQueryRequest request = DeleteByQueryRequest.of(d -> d
                .index(indexName)
                .query(q -> buildQueryFromExample(q, query))
        );

        try {
            DeleteByQueryResponse response = elasticsearchClient.deleteByQuery(request);
            return response.deleted() == null ? 0 : response.deleted().intValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public boolean existsById({{metadata.primaryMetadata.javaType}} id) {
        return selectByPrimaryKey(id) != null;
    }

    
    public boolean existsByExample({{metadata.exampleClazzName}} query) {
        return countByExample(query) > 0;
    }

    
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeysWithSorted(List<{{metadata.primaryMetadata.javaType}}> ids) {
        List<{{metadata.domainClazzName}}> ts = selectByPrimaryKeys(ids);
        // 按原始ID列表顺序排序
        Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> tsMap = ts.stream()
                .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, c -> c));
        return ids.stream()
                .map(tsMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    
    public Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> mapById(List<{{metadata.primaryMetadata.javaType}}> ids) {
        return selectByPrimaryKeys(ids).stream()
                .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, c -> c));
    }

    private void appendLimitSort(SearchRequest.Builder r, {{metadata.exampleClazzName}} q) {
        if (q.getColumns() != null && !q.getColumns().isEmpty()) {
            r.source(sf -> sf.filter(SourceFilter.of(f -> f.includes(q.getColumns()))));
        }
        List<Integer> limit = q.getLimit();
        if (limit != null && !limit.isEmpty()) {
            if (limit.size() == 1) {
                r.size(limit.get(0));
            }
            if (limit.size() == 2) {
                r.from(limit.get(0)).size(limit.get(1));
            }
        }
        String orderByClause = q.getOrderByClause();
        if (orderByClause != null && !orderByClause.isEmpty()) {
            String[] split = orderByClause.split(",");
            List<SortOptions> sorts = new ArrayList<>();
            for (String sort : split) {
                String[] sortSplit = sort.split(" ");
                if (sortSplit.length == 2) {
                    if (sortSplit[1].equalsIgnoreCase("asc")) {
                        sorts.add(SortOptions.of(s -> s.field(f -> f.field(split[0]).order(SortOrder.Asc))));
                    }
                    if (sortSplit[1].equalsIgnoreCase("desc")) {
                        sorts.add(SortOptions.of(s -> s.field(f -> f.field(split[0]).order(SortOrder.Desc))));
                    }
                }
            }
            r.sort(sorts);
        }
    }

    private ObjectBuilder<Query> buildQueryFromExample(Query.Builder q, {{metadata.exampleClazzName}} query) {
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        for ({{metadata.exampleClazzName}}.Criteria criteria : query.getCriteries()) {
            String column = criteria.getColumn();
            String condition = criteria.getCondition();
            Object value = criteria.getValue();
            Object secondValue = criteria.getSecondValue();
            List listValue = criteria.getListValue();

            // 根据不同条件类型构建对应的查询
            if (condition.equals(" = ")) {
                boolQuery.filter(f -> f.term(t -> t.field(column).value(FieldValue.of(value))));
            } else if (condition.equals(" != ") || condition.equals(" <> ")) {
                boolQuery.mustNot(mn -> mn.term(t -> t.field(column).value(FieldValue.of(value))));
            } else if (condition.equals(" >= ")) {
                boolQuery.filter(f -> f.range(r -> r.number(t -> t.gte(toDouble(value)))));
            } else if (condition.equals(" <= ")) {
                boolQuery.filter(f -> f.range(r -> r.number(t -> t.lte(toDouble(value)))));
            } else if (condition.equals(" > ")) {
                boolQuery.filter(f -> f.range(r -> r.number(t -> t.gt(toDouble(value)))));
            } else if (condition.equals(" < ")) {
                boolQuery.filter(f -> f.range(r -> r.number(t -> t.lt(toDouble(value)))));
            } else if (condition.equals(" like ") || condition.equals(" match ")) {
                boolQuery.filter(f -> f.match(w -> w.field(column).query(String.valueOf(value))));
            } else if (condition.equals(" not like ")) {
                boolQuery.mustNot(mn -> mn.match(w -> w.field(column).query(String.valueOf(value))));
            } else if (condition.equals(" matchPhrase ")) {
                boolQuery.mustNot(mn -> mn.matchPhrase(w -> w.field(column).query(String.valueOf(value))));
            } else if (condition.equals(" between ")) {
                // 范围查询：between value1 and value2
                if (value != null && secondValue != null) {
                    NumberRangeQuery numberRangeQuery = new NumberRangeQuery.Builder()
                            .field(column)
                            .gte(toDouble(value))
                            .lt(toDouble(secondValue))
                            .build();
                    boolQuery.filter(mn -> mn.range(r -> r.number(numberRangeQuery)));
                }
            } else if (condition.equals(" not between ")) {
                // 范围查询：between value1 and value2
                if (value != null && secondValue != null) {
                    NumberRangeQuery numberRangeQuery = new NumberRangeQuery.Builder()
                            .field(column)
                            .gte(toDouble(value))
                            .lt(toDouble(secondValue))
                            .build();
                    boolQuery.mustNot(mn -> mn.range(r -> r.number(numberRangeQuery)));
                }
            } else if (condition.equals(" in ")) {
                // 包含查询：添加terms查询
                if (listValue != null && !listValue.isEmpty()) {
                    List<FieldValue> fieldValueList = toFieldValueList(listValue);
                    boolQuery.filter(f -> f.terms(tq -> tq.field(column).terms(tf -> tf.value(
                            fieldValueList
                    ))));
                }
            } else if (condition.equals(" not in ")) {
                // 不包含查询：添加must_not的terms查询
                if (listValue != null && !listValue.isEmpty()) {
                    List<FieldValue> collect = toFieldValueList(listValue);
                    boolQuery.mustNot(f -> f.terms(tq -> tq.field(column).terms(tf -> tf.value(
                            collect
                    ))));
                }
            } else if (condition.equals(" is null ")) {
                // 为空查询：添加exists的must_not查询
                boolQuery.mustNot(mn -> mn.exists(e -> e.field(column)));
            } else if (condition.equals(" is not null ")) {
                // 不为空查询：添加exists查询
                boolQuery.filter(f -> f.exists(e -> e.field(column)));
            }
        }

        // 将布尔查询设置为顶层查询
        return q.bool(boolQuery.build());
    }


    private String buildUpdateScript({{metadata.domainClazzName}} t) {
        List<String> updates = new ArrayList<>();
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            updates.add("ctx._source.{{columnName}} = params.{{columnName}}");
        }
        {{/metadata.columnMetadataList}}
        return String.join("; ", updates);
    }

    private Map<String, JsonData> buildUpdateScriptParams({{metadata.domainClazzName}} t) {
        Map<String, JsonData> params = new HashMap<>();
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            params.put("{{columnName}}", toJsonData(t.get{{firstUpFieldName}}()));
        }
        {{/metadata.columnMetadataList}}
        return params;
    }

    protected double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            return localDateTime.toInstant(ZoneOffset.of(ZoneId.systemDefault().get{{metadata.primaryMetadata.firstUpFieldName}}())).toEpochMilli();
        }
        return Double.parseDouble(value.toString());
    }

    protected JsonData toJsonData(Object value) {
        if (value instanceof Date) {
            return JsonData.of(((Date) value).getTime());
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            return JsonData.of(localDateTime.toInstant(ZoneOffset.of(ZoneId.systemDefault().get{{metadata.primaryMetadata.firstUpFieldName}}())).toEpochMilli());
        }
        return JsonData.of(value);
    }

    protected FieldValue toFieldValue(Object value) {
        return FieldValue.of(value);
    }

    protected List<FieldValue> toFieldValueList(List values) {
        return (List<FieldValue>) values.stream().map(this::toFieldValue).collect(Collectors.toList());
    }

{{#metadata.dataSource}}@org.springframework.beans.factory.annotation.Qualifier("{{metadata.dataSource}}"){{/metadata.dataSource}}
    @Autowired
    public void setElasticsearchClient(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public void setTableName(String tableName) {
        this.indexName = tableName;
    }

}