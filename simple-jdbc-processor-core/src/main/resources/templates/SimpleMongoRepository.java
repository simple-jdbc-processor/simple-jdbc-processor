package {{metadata.packageName}};

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.github.simple.jdbc.processor.SimpleJdbcRepository;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class {{metadata.repositoryClazzSimpleName}} {{#metadata.extendsSimpleJdbcRepository}}implements io.github.simple.jdbc.processor.SimpleJdbcRepository<{{metadata.domainClazzSimpleName}}, {{metadata.primaryMetadata.javaType}}, {{metadata.exampleClazzSimpleName}}> {{/metadata.extendsSimpleJdbcRepository}} {

    private String databaseName = "{{metadata.dataSource}}";

    private String tableName = "{{metadata.tableName}}";

    private MongoCollection<{{metadata.domainClazzName}}> collection;

    private MongoCollection<Document> documentCollection;

    private final Map<String, MongoDatabaseFactory> mongoDatabaseFactoryMap = new HashMap<>();

    private {{metadata.typeHandlerClazzName}} defaultTypeHandler = new {{metadata.typeHandlerClazzName}}();

    
    public {{metadata.domainClazzName}} selectByPrimaryKey({{metadata.primaryMetadata.javaType}} id) {
        Bson filter = Filters.eq("_id", id);
        return getCollection().find(filter).first();
    }

    
    public {{metadata.domainClazzName}} selectByPrimaryKeyForUpdate({{metadata.primaryMetadata.javaType}} id) {
        return selectByPrimaryKey(id);
    }

    
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        Bson filter = Filters.in("_id", ids);
        FindIterable<{{metadata.domainClazzName}}> iterable = getCollection().find(filter);
        List<{{metadata.domainClazzName}}> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    
    public List<{{metadata.domainClazzName}}> selectByExample({{metadata.exampleClazzName}} example) {
        Bson filter = buildFilterFromExample(example);
        FindIterable<{{metadata.domainClazzName}}> iterable;
        MongoCollection<{{metadata.domainClazzName}}> collection = getCollection();
        if (filter != null) {
            iterable = collection.find(filter);
        } else {
            iterable = collection.find();
        }
        appendSortLimitFromExample(example, iterable);

        List<{{metadata.domainClazzName}}> result = new ArrayList<>();
        for ({{metadata.domainClazzName}} t : iterable) {
            result.add(t);
        }
        return result;
    }

    public void consumeByExample({{metadata.exampleClazzName}} example, Consumer<{{metadata.domainClazzName}}> consumer) {
        Bson filter = buildFilterFromExample(example);
        FindIterable<{{metadata.domainClazzName}}> iterable;
        if (filter != null) {
            iterable = getCollection().find(filter);
        } else {
            iterable = getCollection().find();
        }
        appendSortLimitFromExample(example, iterable);
        try (MongoCursor<{{metadata.domainClazzName}}> iterator = iterable.iterator()) {
            while (iterator.hasNext()) {
                consumer.accept(iterator.next());
            }
        }
    }

    
    public {{metadata.domainClazzName}} selectOne({{metadata.exampleClazzName}} example) {
        example.limit(1);
        List<{{metadata.domainClazzName}}> ts = selectByExample(example);
        return ts.isEmpty() ? null : ts.get(0);
    }

    
    public List<{{metadata.domainClazzName}}> selectAll() {
        FindIterable<{{metadata.domainClazzName}}> iterable = getCollection().find();
        List<{{metadata.domainClazzName}}> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    
    public void insert({{metadata.domainClazzName}} t) {
        getCollection().insertOne(t);
    }

    
    public void insertSelective({{metadata.domainClazzName}} t) {
        insert(t);
    }

    
    public void upsertSelective({{metadata.domainClazzName}} t) {
        Bson filter = Filters.eq("_id", t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        Bson update = buildUpdate(t, true);
        getCollection().updateOne(filter, update, new UpdateOptions().upsert(true));
    }

    public void insertIgnoreBatch(List<{{metadata.domainClazzName}}> ts){
        insertBatch(ts);
    }
    
    public void insertBatch(List<{{metadata.domainClazzName}}> ts) {
        if (ts == null || ts.isEmpty()) {
            return;
        }
        getCollection().insertMany(ts);
    }

    
    public long countByExample({{metadata.exampleClazzName}} example) {
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            return getCollection().countDocuments();
        }
        return getCollection().countDocuments(filter);
    }

    
    public int updateByPrimaryKeySelective({{metadata.domainClazzName}} t) {
        Bson filter = Filters.eq("_id", t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        Bson update = buildUpdate(t, true);
        return (int) getCollection().updateOne(filter, update).getModifiedCount();
    }

    
    public int updateByPrimaryKey({{metadata.domainClazzName}} t) {
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null) {
            return 0;
        }
        Bson filter = Filters.eq("_id", t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        Bson update = buildUpdate(t, false);
        return getCollection().updateOne(filter, update).getModifiedCount() > 0 ? 1 : 0;
    }

    
    public int updateByExampleSelective({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} example) {
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null) {
            return 0;
        }
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            throw new IllegalArgumentException("filter is null");
        }
        Bson update = buildUpdate(t, true);
        return (int) getCollection().updateMany(filter, update).getModifiedCount();
    }

    
    public int updateByExample({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} example) {
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            throw new IllegalArgumentException("filter is null");
        }
        Bson update = buildUpdate(t, false);
        return (int) getCollection().updateMany(filter, update).getModifiedCount();
    }

    
    public int deleteByPrimaryKey({{metadata.primaryMetadata.javaType}} id) {
        Bson filter = Filters.eq("_id", id);
        return getCollection().deleteOne(filter).getDeletedCount() > 0 ? 1 : 0;
    }

    
    public int deleteByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        Bson filter = Filters.in("_id", ids);
        return (int) getCollection().deleteMany(filter).getDeletedCount();
    }

    
    public int deleteByExample({{metadata.exampleClazzName}} example) {
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            throw new IllegalArgumentException("filter is null");
        }
        return (int) getCollection().deleteMany(filter).getDeletedCount();
    }

    
    public boolean existsById({{metadata.primaryMetadata.javaType}} id) {
        Bson filter = Filters.eq("_id", id);
        return getCollection().countDocuments(filter) > 0;
    }

    
    public boolean existsByExample({{metadata.exampleClazzName}} example) {
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            throw new IllegalArgumentException("filter is null");
        }
        return getCollection().countDocuments(filter) > 0;
    }

    
    public List<{{metadata.domainClazzName}}> selectByPrimaryKeysWithSorted(List<{{metadata.primaryMetadata.javaType}}> ids) {
        List<{{metadata.domainClazzName}}> ts = selectByPrimaryKeys(ids);
        // 按原始ID列表顺序排序
        Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> m = ts.stream()
                .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, c -> c));
        List<{{metadata.domainClazzName}}> sortTs = new ArrayList<>(ids.size());
        for ({{metadata.primaryMetadata.javaType}} id : ids) {
            if (m.containsKey(id)) {
                sortTs.add(m.get(id));
            }
        }
        return sortTs;
    }

    
    public Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> mapById(List<{{metadata.primaryMetadata.javaType}}> ids) {
        List<{{metadata.domainClazzName}}> ts = selectByPrimaryKeys(ids);
        return ts.stream()
                .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, c -> c));
    }

    public <T> List<T> aggregate({{metadata.exampleClazzName}} example, List<Bson> pipeline, Function<Document, T> function) {
        List<Bson> pipelineList;
        if (example != null) {
            pipelineList = new ArrayList<>();
            Bson bson = buildFilterFromExample(example);
            pipelineList.add(bson);
        } else {
            pipelineList = pipeline;
        }
        List<T> list = new ArrayList<>();
        // 执行聚合查询
        try (MongoCursor<Document> cursor = getDocumentCollection()
                .aggregate(pipelineList)
                .cursor()) {
            // 遍历结果
            while (cursor.hasNext()) {
                T apply = function.apply(cursor.next());
                list.add(apply);
            }
        }
        return list;
    }


    public <T> List<T> aggregate(List<Bson> pipeline, Function<Document, T> function) {
        return aggregate(null, pipeline, function);
    }

    public <T> T aggregateOne({{metadata.exampleClazzName}} example, List<Bson> pipeline, Function<Document, T> function) {
        List<T> aggregate = aggregate(example, pipeline, function);
        return aggregate.isEmpty() ? null : aggregate.get(0);
    }

    public <T> T aggregateOne(List<Bson> pipeline, Function<Document, T> function) {
        return aggregateOne(null, pipeline, function);
    }

    protected Bson buildFilterFromExample({{metadata.exampleClazzName}} example) {
        List<List<{{metadata.exampleClazzName}}.Criteria>> all;
        if (example.getOrConditions() != null && !example.getOrConditions().isEmpty()) {
            all = new ArrayList<>(example.getOrConditions());
        } else {
            if (example.getCriteries().isEmpty()) {
                return null;
            }
            all = Collections.singletonList(example.getCriteries());
        }

        List<Bson> filters = new ArrayList<>();
        for (List<{{metadata.exampleClazzName}}.Criteria> orCondition : all) {
            List<Bson> subFilters = new ArrayList<>();
            for ({{metadata.exampleClazzName}}.Criteria critery : orCondition) {
                String column = critery.getColumn();
                String condition = critery.getCondition();
                if (condition.equals(" = ")) {
                    subFilters.add(Filters.eq(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" != ") || condition.equals(" <> ")) {
                    subFilters.add(Filters.ne(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" >= ")) {
                    subFilters.add(Filters.gte(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" <= ")) {
                    subFilters.add(Filters.lte(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" > ")) {
                    subFilters.add(Filters.gt(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" < ")) {
                    subFilters.add(Filters.lt(column, defaultTypeHandler.encode(column,critery.getValue())));
                } else if (condition.equals(" like ")) {
                    subFilters.add(Filters.regex(column, (String) defaultTypeHandler.encode(column,critery.getValue().toString())));
                } else if (condition.equals(" between ")) {
                    subFilters.add(Filters.gte(column, defaultTypeHandler.encode(column,critery.getValue())));
                    subFilters.add(Filters.lte(column, defaultTypeHandler.encode(column,critery.getSecondValue())));
                } else if (condition.equals(" not between ")) {
                    subFilters.add(Filters.lt(column, defaultTypeHandler.encode(column,critery.getValue())));
                    subFilters.add(Filters.gt(column, defaultTypeHandler.encode(column,critery.getSecondValue())));
                } else if (condition.equals(" in ")) {
                    subFilters.add(Filters.in(column, defaultTypeHandler.encodeList(column,critery.getListValue())));
                } else if (condition.equals(" not in ")) {
                    subFilters.add(Filters.nin(column, defaultTypeHandler.encodeList(column,critery.getListValue())));
                } else if (condition.equals(" not like ")) {
                    subFilters.add(Filters.not(Filters.regex(column,(String) defaultTypeHandler.encode(column,critery.getValue().toString()))));
                } else if (condition.equals(" is null ")) {
                    subFilters.add(Filters.eq(column, null));
                } else if (condition.equals(" is not null ")) {
                    subFilters.add(Filters.ne(column, null));
                }
            }
            filters.add(Filters.and(subFilters));
        }

        if (filters.size() == 1) {
            return filters.get(0);
        }

        return filters.isEmpty() ? null : Filters.or(filters);
    }


    private void appendSortLimitFromExample({{metadata.exampleClazzName}} example, FindIterable<{{metadata.domainClazzName}}> iterable) {
        if (example.getOrderByClause() != null && !example.getOrderByClause().isEmpty()) {
            String[] split = example.getOrderByClause().split(",");
            List<Bson> sorts = new ArrayList<>();
            for (String sort : split) {
                String[] sortSplit = sort.split(" ");
                if (sortSplit.length == 2) {
                    if (sortSplit[1].equalsIgnoreCase("asc")) {
                        sorts.add(Sorts.ascending(sortSplit[0]));
                    }
                    if (sortSplit[1].equalsIgnoreCase("desc")) {
                        sorts.add(Sorts.descending(sortSplit[0]));
                    }
                }
            }
            if (!sorts.isEmpty()) {
                iterable.sort(Sorts.orderBy(sorts));
            }
        }
        if (example.getColumns() != null && !example.getColumns().isEmpty()) {
            iterable.projection(Projections.include(example.getColumns()));
        }

        List<Integer> limit = example.getLimit();
        if (limit != null && !limit.isEmpty()) {
            if (limit.size() == 1) {
                iterable.limit(limit.get(0));
            }
            if (limit.size() == 2) {
                iterable.limit(limit.get(1))
                        .skip(limit.get(0));
            }
        }
    }

    public int incrementByExample({{metadata.exampleClazzName}} example, String column, Number amount) {
        Bson filter = buildFilterFromExample(example);
        if (filter == null) {
            throw new IllegalArgumentException("filter is null");
        }
        MongoCollection<{{metadata.domainClazzName}}> collection = getCollection();
        Document update = new Document("$inc", new Document(column, amount));
        return (int) collection.updateMany(filter, update).getModifiedCount();
    }

    private Bson buildUpdate({{metadata.domainClazzName}} t, boolean selective) {
        List<Bson> updateOperations = new ArrayList<>();

        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null || !selective) {
            updateOperations.add(Updates.set("{{columnName}}", t.get{{firstUpFieldName}}()));
        }
        {{/metadata.columnMetadataList}}
        return Updates.combine(updateOperations);
    }

    protected MongoCollection<{{metadata.domainClazzName}}> getCollection() {
        return getCollection(this.tableName, {{metadata.domainClazzName}}.class);
    }

    protected MongoCollection<Document> getDocumentCollection() {
        return getCollection(this.tableName, Document.class);
    }

    protected <T> MongoCollection<T> getCollection(String collectionName, Class<T> clazz) {
        MongoDatabaseFactory mongoDatabaseFactory = getMongoDatabaseFactory(databaseName);
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            MongoDatabase mongoDatabase = MongoDatabaseUtils.getDatabase(mongoDatabaseFactory);
            return mongoDatabase.getCollection(collectionName, clazz)
                    .withCodecRegistry(defaultTypeHandler.getSelftCodecRegistry());
        }
        if (collection == null) {
            MongoDatabase mongoDatabase = mongoDatabaseFactory.getMongoDatabase(databaseName);
            collection = mongoDatabase.getCollection(this.tableName, {{metadata.domainClazzName}}.class)
                    .withCodecRegistry(defaultTypeHandler.getSelftCodecRegistry());
            documentCollection = mongoDatabase.getCollection(this.tableName, Document.class)
                    .withCodecRegistry(defaultTypeHandler.getSelftCodecRegistry());
        }
        if (clazz == {{metadata.domainClazzName}}.class) {
            return (MongoCollection<T>) collection;
        }
        if (clazz == Document.class) {
            return (MongoCollection<T>) documentCollection;
        }

        return getCollection(this.tableName, clazz)
                .withCodecRegistry(defaultTypeHandler.getSelftCodecRegistry());
    }

    protected MongoDatabaseFactory getMongoDatabaseFactory(String databaseName) {
        MongoDatabaseFactory mongoDatabaseFactory = mongoDatabaseFactoryMap.get(databaseName);
        if(mongoDatabaseFactory != null) {
            return mongoDatabaseFactory;
        }
        for (Map.Entry<String, MongoDatabaseFactory> entry : mongoDatabaseFactoryMap.entrySet()) {
            MongoDatabase mongoDatabase = entry.getValue().getMongoDatabase(databaseName);
            if(mongoDatabase != null) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("MongoDatabaseFactory not found for database " + databaseName);
    }

    protected MongoDatabaseFactory getMongoDatabaseFactory() {
        return mongoDatabaseFactoryMap.get(this.databaseName);
    }

    @Autowired
    public synchronized void setMongoDatabaseFactory(Map<String, MongoDatabaseFactory> factoryMap) {
        this.defaultTypeHandler = new {{metadata.typeHandlerClazzName}}();
        for (Map.Entry<String, MongoDatabaseFactory> entry : factoryMap.entrySet()) {
            MongoDatabaseFactory factory = entry.getValue();
            String name = factory.getMongoDatabase().getName();
            this.mongoDatabaseFactoryMap.put(name, factory);
            if (this.databaseName.equalsIgnoreCase(name)) {
                this.defaultTypeHandler = new {{metadata.typeHandlerClazzName}}(factory.getCodecRegistry());
            }
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public synchronized void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }
    public synchronized void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
