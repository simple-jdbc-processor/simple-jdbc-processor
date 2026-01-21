package {{metadata.packageName}};


import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class {{metadata.repositoryClazzSimpleName}} {{#metadata.extendsSimpleJdbcRepository}}implements io.github.simple.jdbc.processor.SimpleJdbcRepository<{{metadata.domainClazzName}}, {{metadata.primaryMetadata.javaType}}, {{metadata.exampleClazzName}}> {{/metadata.extendsSimpleJdbcRepository}}{

    private final String tableName;

    private final String columnsStr = "{{metadata.columns}}";

    private final String primaryKeyStr = "{{metadata.primaryMetadata.columnName}}";

    private final List<String> columns = Arrays.asList(columnsStr.split(", "));

    protected final org.slf4j.Logger log;

    private final java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong();

    private DataSource dataSource;

    private List<String> slaveDataSources  = new ArrayList<>();

    private Map<String, DataSource> dataSourceMap = new HashMap<>();

    private {{metadata.typeHandlerClazzName}} defaultTypeHandler = new {{metadata.typeHandlerClazzName}}();

    private final Class<{{metadata.primaryMetadata.javaType}}> primaryKeyType = {{metadata.primaryMetadata.javaType}}.class;

    private final String primaryKeyCondition;

    private final String primaryKeyInCondition;

    private final String selectByPrimaryKeySql;

    private final String selectByPrimaryKeyForUpdateSql;

    private final String selectByPrimaryKeysSql;

    private final String updatePrefix;

    private final String updateSuffix;

    private final String updateByExamplePrefix;

    private final String updateByPrimaryKeySql;

    private final String deletePrefix;

    private final String deleteByPrimaryKeySql;

    private final String deleteByPrimaryKeysSql;

    private final String insertSqlPrefix;

    private final String insertIgnoreSqlPrefix;

    private final String insertSql;

    private final String insertPlaceHolderSuffix;


    public {{metadata.repositoryClazzSimpleName}}() {
        this.tableName = "{{metadata.tableName}}";
        this.log = org.slf4j.LoggerFactory.getLogger(this.getClass());
        this.primaryKeyCondition = " where " + primaryKeyStr + " = ?";
        this.primaryKeyInCondition = " where " + primaryKeyStr + " in ";
        this.selectByPrimaryKeySql = "select " + columnsStr + " from " + tableName + primaryKeyCondition;
        this.selectByPrimaryKeyForUpdateSql = this.selectByPrimaryKeySql + " for update";
        this.selectByPrimaryKeysSql = "select " + columnsStr + " from " + tableName + primaryKeyInCondition;
        this.updatePrefix = "update " + tableName;
        this.updateSuffix = " set " + String.join(" = ?, ", columnsStr.split(",")) + " = ?";
        this.updateByExamplePrefix = updatePrefix + updateSuffix;
        this.updateByPrimaryKeySql = updatePrefix + updateSuffix + primaryKeyCondition;
        this.deletePrefix = "delete from " + tableName;
        this.deleteByPrimaryKeySql = deletePrefix + primaryKeyCondition;
        this.deleteByPrimaryKeysSql = deletePrefix + primaryKeyInCondition;
        this.insertSqlPrefix = "insert into " + tableName + " (" + columnsStr + ") values ";
        this.insertIgnoreSqlPrefix = "insert ignore into " + tableName + " (" + columnsStr + ") values ";
        this.insertPlaceHolderSuffix = appendPlaceholder(columns.size());;
        this.insertSql = insertSqlPrefix + insertPlaceHolderSuffix;
    }

{{#metadata.shard}}
    public {{metadata.repositoryClazzSimpleName}}(Class<?> logClazz, String tableName, DataSource masterDataSource, Map<String, DataSource> slaveDataSourceMap, {{metadata.typeHandlerClazzName}} defaultTypeHandler){
        this.log = org.slf4j.LoggerFactory.getLogger(logClazz);
        this.tableName = tableName;
        this.dataSource = masterDataSource;
        this.defaultTypeHandler = defaultTypeHandler;
        this.dataSourceMap = slaveDataSourceMap;
        this.slaveDataSources = new ArrayList<>(slaveDataSourceMap.keySet());
        this.primaryKeyCondition = " where " + primaryKeyStr + " = ?";
        this.primaryKeyInCondition = " where " + primaryKeyStr + " in ";
        this.selectByPrimaryKeySql = "select " + columnsStr + " from " + tableName + primaryKeyCondition;
        this.selectByPrimaryKeyForUpdateSql = this.selectByPrimaryKeySql + " for update";
        this.selectByPrimaryKeysSql = "select " + columnsStr + " from " + tableName + primaryKeyInCondition;
        this.updatePrefix = "update " + tableName;
        this.updateSuffix = " set " + String.join(" = ?, ", columnsStr.split(",")) + " = ?";
        this.updateByExamplePrefix = updatePrefix + updateSuffix;
        this.updateByPrimaryKeySql = updatePrefix + updateSuffix + primaryKeyCondition;
        this.deletePrefix = "delete from " + tableName;
        this.deleteByPrimaryKeySql = deletePrefix + primaryKeyCondition;
        this.deleteByPrimaryKeysSql = deletePrefix + primaryKeyInCondition;
        this.insertSqlPrefix = "insert into " + tableName + " (" + columnsStr + ") values ";
        this.insertIgnoreSqlPrefix = "insert ignore into " + tableName + " (" + columnsStr + ") values ";
        this.insertPlaceHolderSuffix = appendPlaceholder(columns.size());;
        this.insertSql = insertSqlPrefix + insertPlaceHolderSuffix;
    }
{{/metadata.shard}}

    public {{metadata.domainClazzName}} selectByPrimaryKey({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        return selectOne(selectByPrimaryKeySql, Collections.singletonList({{metadata.primaryMetadata.fieldName}}), r-> defaultTypeHandler.handle(r));
    }

    public {{metadata.domainClazzName}} selectByPrimaryKeyForUpdate({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        return selectOne(selectByPrimaryKeyForUpdateSql, Collections.singletonList({{metadata.primaryMetadata.fieldName}}), r-> defaultTypeHandler.handle(r));
    }

    public List<{{metadata.domainClazzName}}> selectByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> {{metadata.primaryMetadata.fieldName}}s) {
        String sql = selectByPrimaryKeysSql + appendPlaceholder({{metadata.primaryMetadata.fieldName}}s.size());
        return selectList(sql, {{metadata.primaryMetadata.fieldName}}s, r-> defaultTypeHandler.handle(r));
    }

    public int updateByPrimaryKey({{metadata.domainClazzName}} t) {
        defaultTypeHandler.preUpdate(t);
        ArrayList<Object> params = new ArrayList<>(columns.size() + 1);
        defaultTypeHandler.encode(params, t);
        params.add(t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        int affect = update(updateByPrimaryKeySql, params);
        defaultTypeHandler.afterUpdate(t);
        return affect;
    }

    public List<{{metadata.domainClazzName}}> selectAll() {
        return selectByExample({{metadata.exampleClazzName}}.create());
    }


    public boolean existsById({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}){
        return selectByPrimaryKey({{metadata.primaryMetadata.fieldName}}) != null;
    }

    public boolean existsByExample({{metadata.exampleClazzName}} example) {
        return selectOne(example) != null;
    }


    public int updateByPrimaryKeySelective({{metadata.domainClazzName}} t) {
        defaultTypeHandler.preUpdate(t);
        StringBuilder prefix = new StringBuilder(updateByPrimaryKeySql.length())
                .append("update ")
                .append(tableName)
                .append(" set ");
        ArrayList<Object> params = new ArrayList<>(columns.size() + 1);
        defaultTypeHandler.encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if ({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}) {
            prefix.append("{{columnName}} = ?, ");
        }
        {{/metadata.columnMetadataList}}
        params.add(t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        prefix = prefix.delete(prefix.length() - 2, prefix.length());
        String sql = prefix.append(primaryKeyCondition).toString();
        int affect = update(sql, params);
        defaultTypeHandler.afterUpdate(t);
        return affect;
    }

    public int deleteByPrimaryKey({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        return delete(deleteByPrimaryKeySql, java.util.Collections.singletonList({{metadata.primaryMetadata.fieldName}}));
    }

    public int deleteByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> {{metadata.primaryMetadata.fieldName}}s) {
        String sql = deleteByPrimaryKeysSql + appendPlaceholder({{metadata.primaryMetadata.fieldName}}s.size());
        return delete(sql, {{metadata.primaryMetadata.fieldName}}s);
    }

    public {{metadata.domainClazzName}} selectOne({{metadata.exampleClazzName}} example) {
        example.limit(1);
        List<{{metadata.domainClazzName}}> ts = selectByExample(example);
        return ts.isEmpty() ? null : ts.get(0);
    }

    public List<{{metadata.domainClazzName}}> selectByExample({{metadata.exampleClazzName}} example) {
        String sql = toSelectByExampleSql(example);
        List<String> columns = example.getColumns();
        if (columns == null || columns.isEmpty()) {
            return selectList(sql, getConditionValues(example), r-> defaultTypeHandler.handle(r));
        }
        return selectList(sql, getConditionValues(example), rs -> defaultTypeHandler.handle(rs, columns));
    }

    public <R> Map<R, {{metadata.domainClazzName}}> mapByExample({{metadata.exampleClazzName}} example, Function<{{metadata.domainClazzName}}, R> keyMapper) {
        List<{{metadata.domainClazzName}}> ts = selectByExample(example);
        return ts.stream().collect(Collectors.toMap(keyMapper, Function.identity()));
    }

    public long countByExample({{metadata.exampleClazzName}} example) {
        String sql = toCountByExampleSql(example);
        return selectOne(sql, getConditionValues(example), rs -> rs.getLong(1));
    }

    public int updateByExample({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} example) {
        ArrayList<Object> params;
        if (example.getUpdateSetValues() != null) {
            params = new ArrayList<>(example.getUpdateSetValues());
        } else {
            params = new ArrayList<>(columns.size());
        }

        defaultTypeHandler.encode(params, t);
        String condition = toConditionSql(example);
        String sql;
        if (example.getUpdateExpression() != null) {
            sql = updateByExamplePrefix + String.join(", ", example.getUpdateExpression()) + condition;
        } else {
            sql = updateByExamplePrefix + condition;
        }
        params.addAll(getConditionValues(example));
        return update(sql, params);
    }

    public int updateByExampleSelective({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} example) {
        StringBuilder prefix = new StringBuilder(updateByPrimaryKeySql.length() + 32)
                .append("update ")
                .append(tableName)
                .append(" set ");

        ArrayList<Object> params;
        if (example.getUpdateSetValues() != null) {
            params = new ArrayList<>(example.getUpdateSetValues());
        } else {
            params = new ArrayList<>(columns.size());
        }

        if (example.getUpdateExpression() != null) {
            prefix.append(String.join(", ", example.getUpdateExpression()));
        }

        defaultTypeHandler.encodeSelective(params, t);

        {{#metadata.columnMetadataList}}
        if ({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}) {
            prefix.append("{{columnName}} = ?, ");
        }
        {{/metadata.columnMetadataList}}
        params.addAll(getConditionValues(example));
        prefix = prefix.delete(prefix.length() - 2, prefix.length());
        String sql = prefix.append(toConditionSql(example)).toString();
        return update(sql, params);
    }

    public int updateByExampleSelective({{metadata.exampleClazzName}} example) {
        StringBuilder prefix = new StringBuilder(updateByPrimaryKeySql.length() + 32)
                .append("update ")
                .append(tableName)
                .append(" set ");

        ArrayList<Object> params;
        if (example.getUpdateSetValues() != null) {
            params = new ArrayList<>(example.getUpdateSetValues());
        } else {
            params = new ArrayList<>(columns.size());
        }

        if (example.getUpdateExpression() != null) {
            prefix.append(String.join(", ", example.getUpdateExpression()));
        }

        params.addAll(getConditionValues(example));
        String sql = prefix + toConditionSql(example);
        return update(sql, params);
    }


    public int[] updateBatchByExample(List<{{metadata.exampleClazzName}}> examples) {
        LinkedHashMap<String,List<Object[]>> m = new LinkedHashMap<>();
        for({{metadata.exampleClazzName}} example: examples){
            StringBuilder prefix = new StringBuilder(updateByPrimaryKeySql.length() + 32)
                    .append("update ")
                    .append(tableName)
                    .append(" set ");
            ArrayList<Object> params;
            if (example.getUpdateSetValues() != null) {
                params = new ArrayList<>(example.getUpdateSetValues());
            } else {
                params = new ArrayList<>(columns.size());
            }

            if (example.getUpdateExpression() != null) {
                prefix.append(String.join(", ", example.getUpdateExpression()));
            }
            params.addAll(getConditionValues(example));
            String sql = prefix + toConditionSql(example);
            m.computeIfAbsent(sql, k-> new ArrayList<>()).add(params.toArray());
        }
        int[] affect = new int[examples.size()];
        int i = 0;
        for(Map.Entry<String,List<Object[]>> e: m.entrySet()){
            int[] affects = updateBatch(e.getKey(), e.getValue());
            for(int a: affects){
                affect[i++] = a;
            }
        }
        return affect;
    }

    public void insert({{metadata.domainClazzName}} t) {
        defaultTypeHandler.generatePrimaryKey(t);
        defaultTypeHandler.preInsert(t);
        ArrayList<Object> params = new ArrayList<>(columns.size());
        defaultTypeHandler.encode(params, t);
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(insertSql, params);
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        defaultTypeHandler.afterInsert(t);
    }

    public void insertBatch(List<{{metadata.domainClazzName}}> ts, int batchSize) {
        if (ts == null || ts.isEmpty()) {
            throw new IllegalArgumentException("ts is null or empty");
        }
        int pages = ts.size() / batchSize;
        for (int i = 0; i < pages; i++) {
            int start = i * batchSize;
            int end = start + batchSize;
            List<{{metadata.domainClazzName}}> subList = ts.subList(start, end);
            insertBatch(subList);
        }
        if (ts.size() % batchSize != 0) {
            List<{{metadata.domainClazzName}}> subList = ts.subList(pages * batchSize, ts.size());
            insertBatch(subList);
        }
    }

    public void insertIgnoreBatch(List<{{metadata.domainClazzName}}> ts, int batchSize) {
        if (ts == null || ts.isEmpty()) {
            throw new IllegalArgumentException("ts is null or empty");
        }
        int pages = ts.size() / batchSize;
        for (int i = 0; i < pages; i++) {
            int start = i * batchSize;
            int end = start + batchSize;
            List<{{metadata.domainClazzName}}> subList = ts.subList(start, end);
            insertIgnoreBatch(subList);
        }
        if (ts.size() % batchSize != 0) {
            List<{{metadata.domainClazzName}}> subList = ts.subList(pages * batchSize, ts.size());
            insertIgnoreBatch(subList);
        }
    }

    public void insertBatch(List<{{metadata.domainClazzName}}> ts) {
        if (ts == null || ts.isEmpty()) {
            throw new IllegalArgumentException("ts is null or empty");
        }
        if (ts.size() == 1) {
            insertSelective(ts.get(0));
            return;
        }
        defaultTypeHandler.batchGeneratePrimaryKey(ts);
        ArrayList<Object> params = new ArrayList<>(columns.size() * ts.size());

        int initCapacity = insertSqlPrefix.length() + 32 + (ts.size() * 2) + (insertPlaceHolderSuffix.length() * ts.size());
        StringBuilder sb = new StringBuilder(initCapacity)
            .append(insertSqlPrefix);
        for ({{metadata.domainClazzName}} t : ts) {
            sb.append(insertPlaceHolderSuffix);
            sb.append(", ");
            defaultTypeHandler.preInsert(t);
            defaultTypeHandler.encode(params, t);
        }
        String sql = sb.delete(sb.length() - 2, sb.length()).toString();
        sb = null;
        List<{{metadata.primaryMetadata.javaType}}> primaryKeys = insertBatch(sql, params);
        params = null;
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        int index = 0;
        for (int i = 0; i < ts.size(); i++) {
            {{metadata.domainClazzName}} t = ts.get(i);
            if(t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null){
                ts.get(index).set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKeys.get(index));
                index++;
            }
            defaultTypeHandler.afterInsert(t);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        {{^metadata.primaryMetadata.useGeneratedKeys}}
        for (int i = 0; i < ts.size(); i++) {
            {{metadata.domainClazzName}} t = ts.get(i);
            defaultTypeHandler.afterInsert(t);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
    }

    public void insertIgnoreBatch(List<{{metadata.domainClazzName}}> ts) {
        if (ts == null || ts.isEmpty()) {
            throw new IllegalArgumentException("ts is null or empty");
        }
        if (ts.size() == 1) {
            insertSelective(ts.get(0));
            return;
        }
        defaultTypeHandler.batchGeneratePrimaryKey(ts);
        ArrayList<Object> params = new ArrayList<>(columns.size() * ts.size());

        int initCapacity = insertSqlPrefix.length() + 32 + (ts.size() * 2) + (insertPlaceHolderSuffix.length() * ts.size());
        StringBuilder sb = new StringBuilder(initCapacity)
            .append(insertIgnoreSqlPrefix);
        for ({{metadata.domainClazzName}} t : ts) {
            sb.append(insertPlaceHolderSuffix);
            sb.append(", ");
            defaultTypeHandler.preInsert(t);
            defaultTypeHandler.encode(params, t);
        }
        String sql = sb.delete(sb.length() - 2, sb.length()).toString();
        sb = null;
        List<{{metadata.primaryMetadata.javaType}}> primaryKeys = insertBatch(sql, params);
        params = null;
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        for (int i = 0; i < ts.size(); i++) {
            {{metadata.domainClazzName}} t = ts.get(i);
            defaultTypeHandler.afterInsert(t);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        {{^metadata.primaryMetadata.useGeneratedKeys}}
        for (int i = 0; i < ts.size(); i++) {
            {{metadata.domainClazzName}} t = ts.get(i);
            defaultTypeHandler.afterInsert(t);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
    }

    public void insertSelective({{metadata.domainClazzName}} t) {
        defaultTypeHandler.generatePrimaryKey(t);
        defaultTypeHandler.preInsert(t);
        ArrayList<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("insert into ")
                .append(tableName)
                .append(" (");
        defaultTypeHandler.encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if ({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}

        prefix = prefix.delete(prefix.length() - 2, prefix.length());
        String sql = prefix.append( ") values ").append(appendPlaceholder(params.size())).toString() ;
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        defaultTypeHandler.afterInsert(t);
    }


    public void upsertSelective({{metadata.domainClazzName}} t) {
        defaultTypeHandler.generatePrimaryKey(t);
        defaultTypeHandler.preInsert(t);
        ArrayList<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("replace into ")
                .append(tableName)
                .append(" (");

        defaultTypeHandler.encodeSelective(params, t);

        {{#metadata.columnMetadataList}}
        if ({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}


        String sql = prefix.delete(prefix.length() - 2, prefix.length()).append( ") values ")
                .append(appendPlaceholder(params.size())).toString() ;
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        defaultTypeHandler.afterInsert(t);
    }


    public void insertIgnoreSelective({{metadata.domainClazzName}} t) {
        defaultTypeHandler.generatePrimaryKey(t);
        defaultTypeHandler.preInsert(t);
        ArrayList<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("insert ignore into ")
                .append(tableName)
                .append(" (");

        defaultTypeHandler.encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if ({{^basicType}}t.get{{firstUpFieldName}}() != null{{/basicType}}{{#basicType}}true{{/basicType}}) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}


        String sql = prefix.delete(prefix.length() - 2, prefix.length()).append( ") values ")
                .append(appendPlaceholder(params.size())).toString() ;
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        {{#metadata.primaryMetadata.useGeneratedKeys}}
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata.useGeneratedKeys}}
        defaultTypeHandler.afterInsert(t);
    }


    public int deleteByExample({{metadata.exampleClazzName}} example) {
        String sql = deletePrefix + toConditionSql(example);
        return delete(sql, getConditionValues(example));
    }

    protected <T> List<T> selectList(String sql, List params, Handler<T> handler) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postSelect(sql, params);
        ArrayList<T> list = new ArrayList<>();
        Connection connection = getConnection(true);
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            defaultTypeHandler.setParameters(statement, params);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    T result = handler.handle(resultSet);
                    list.add(result);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        if (log.isDebugEnabled()) {
            log.debug("Total:      {}", list.size());
        }
        return list;
    }

    public void consumeByExample({{metadata.exampleClazzName}} example, Consumer<{{metadata.domainClazzName}}> consume) {
        String sql = toSelectByExampleSql(example);
        List<String> columns = example.getColumns();
        if (columns == null || columns.isEmpty()) {
            consume(sql, getConditionValues(example), r-> defaultTypeHandler.handle(r),consume);
            return;
        }
        consume(sql, getConditionValues(example), rs -> defaultTypeHandler.handle(rs, columns), consume);
    }


    protected <T> void consume(String sql, List params, Handler<T> handler, Consumer<T> consumer) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postSelect(sql, params);
        ArrayList<T> list = new ArrayList<>();
        Connection connection = getConnection(true);
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        boolean autoCommit;
        try {
            autoCommit = connection.getAutoCommit();
            if (!autoCommit) {
                connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long total = 0;
        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(Integer.MIN_VALUE);
            defaultTypeHandler.setParameters(statement, params);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    T handle = handler.handle(resultSet);
                    consumer.accept(handle);
                    total++;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (autoCommit) {
                closeCheckTx(connection);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Total:      {}", total);
        }
    }


    public List<{{metadata.domainClazzName}}> selectByPrimaryKeysWithSorted(List<{{metadata.primaryMetadata.javaType}}> ids) {
        List<{{metadata.domainClazzName}}> ts = selectByPrimaryKeys(ids);
        if (!ts.isEmpty()) {
            Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> m = ts.stream()
                    .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, java.util.function.Function.identity()));
            return ids.stream()
                    .map(m::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return ts;
    }

    public Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> mapById(List<{{metadata.primaryMetadata.javaType}}> ids) {
        return mapById(ids, {{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}});
    }

    public <R> Map<R, {{metadata.domainClazzName}}> mapById(List<{{metadata.primaryMetadata.javaType}}> ids, java.util.function.Function<{{metadata.domainClazzName}}, R> function) {
        return selectByPrimaryKeys(ids)
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(function, java.util.function.Function.identity()));
    }


    protected <T> T selectOne(String sql, List params, Handler<T> handler) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postSelect(sql, params);
        Connection connection = getConnection(true);
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            defaultTypeHandler.setParameters(statement, params);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Total:      {}", 1);
                    }
                    return handler.handle(resultSet);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Total:      {}", 0);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        return null;
    }

    protected int update(String sql, List params) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postUpdate(sql , params);
        Connection connection = getConnection();

        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            defaultTypeHandler.setParameters(statement, params);
            int affect = statement.executeUpdate();
            if (log.isDebugEnabled()) {
                log.debug("Total:      {}", affect);
            }
            return affect;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
    }

    protected int[] updateBatch(String sql, List<Object[]> batchParams) {
        Connection connection = getConnection();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Object[] batchParam : batchParams) {
                defaultTypeHandler.auditSql(log, sql, batchParam);
                defaultTypeHandler.postUpdate(sql , batchParam);
                if (log.isDebugEnabled()) {
                    log.debug("Preparing:  {}", sql);
                    log.debug("Parameters: {}", paramsToString(batchParam));
                }
                defaultTypeHandler.setParameters(statement, batchParam);
                statement.addBatch();
            }
            int[] affects = statement.executeBatch();
            if (log.isDebugEnabled()) {
                for (int affect : affects) {
                    log.debug("Total:      {}", affect);
                }
            }
            return affects;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
    }

    protected int delete(String sql, List params) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postDelete(sql , params);
        Connection connection = getConnection();
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            defaultTypeHandler.setParameters(statement, params);
            int affect = statement.executeUpdate();
            if (log.isDebugEnabled()) {
                log.debug("Total:      {}", affect);
            }
            return affect;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
    }

    protected {{metadata.primaryMetadata.javaType}} insert(String sql, List params) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postInsert(sql , params);
        Connection connection = getConnection();
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        try (PreparedStatement statement = connection.prepareStatement(sql{{#metadata.primaryMetadata.useGeneratedKeys}}, Statement.RETURN_GENERATED_KEYS{{/metadata.primaryMetadata.useGeneratedKeys}})) {
            defaultTypeHandler.setParameters(statement, params);
            int affect = statement.executeUpdate();
            {{#metadata.primaryMetadata.useGeneratedKeys}}
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getObject(1, {{metadata.primaryMetadata.javaType}}.class);
            }
            {{/metadata.primaryMetadata.useGeneratedKeys}}
            if (log.isDebugEnabled()) {
                log.debug("Total:      {}", affect);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        return {{metadata.primaryMetadata.defaultValue}};
    }


    protected List<{{metadata.primaryMetadata.javaType}}> insertBatch(String sql, List<Object> params) {
        defaultTypeHandler.auditSql(log, sql, params);
        defaultTypeHandler.postInsert(sql , params);
        Connection connection = getConnection();
        if (log.isDebugEnabled()) {
            log.debug("Preparing:  {}", sql);
            log.debug("Parameters: {}", paramsToString(params));
        }
        List<{{metadata.primaryMetadata.javaType}}> ids = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql{{#metadata.primaryMetadata.useGeneratedKeys}}, Statement.RETURN_GENERATED_KEYS{{/metadata.primaryMetadata.useGeneratedKeys}})) {
            defaultTypeHandler.setParameters(statement, params);
            int affect = statement.executeUpdate();
            {{#metadata.primaryMetadata.useGeneratedKeys}}
            ResultSet generatedKeys = statement.getGeneratedKeys();
            while (generatedKeys.next()) {
                {{metadata.primaryMetadata.javaType}} primaryKey = generatedKeys.getObject(1, {{metadata.primaryMetadata.javaType}}.class);
                ids.add(primaryKey);
            }
            {{/metadata.primaryMetadata.useGeneratedKeys}}

            if (log.isDebugEnabled()) {
                log.debug("Total:      {}", affect);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        return ids;
    }

    public <T> List<T> aggregate({{metadata.exampleClazzSimpleName}} example, Handler<T> handler) {
        StringBuilder sb = new StringBuilder("select ")
                .append(String.join(", ", example.getAggregates()))
                .append(" from ")
                .append(tableName)
                .append(toConditionSql(example));

        return selectList(sb.toString(), getConditionValues(example), handler);
    }

    public <T> T aggregateOne({{metadata.exampleClazzSimpleName}} example, Handler<T> handler) {
        StringBuilder sb = new StringBuilder("select ")
                .append(String.join(", ", example.getAggregates()))
                .append(" from ")
                .append(tableName)
                .append(toConditionSql(example));

        return selectOne(sb.toString(), getConditionValues(example), handler);
    }


    protected void closeCheckTx(Connection connection) {
        try {
            if (connection != null && !connection.isClosed() && connection.getAutoCommit()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void close(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected interface Handler<R> {
        R handle(ResultSet rs) throws SQLException;
    }


    protected String appendPlaceholder(int size) {
        StringBuilder sql = new StringBuilder(size * 2 + 2);
        sql.append("(");
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                sql.append("?");
            } else {
                sql.append(",?");
            }
        }
        sql.append(")");
        return sql.toString();
    }

    protected List getConditionValues({{metadata.exampleClazzName}} example) {
        ArrayList<Object> params = new ArrayList<>();
        List<List<{{metadata.exampleClazzSimpleName}}.Criteria>> orConditions = example.getOrConditions();
        if (orConditions != null && !orConditions.isEmpty()) {
            for (List<{{metadata.exampleClazzSimpleName}}.Criteria> orCondition : orConditions) {
                for ({{metadata.exampleClazzSimpleName}}.Criteria criteria : orCondition) {
                    convertConditionParam(params, criteria);
                }
            }
        } else {
            for ({{metadata.exampleClazzSimpleName}}.Criteria criteria : example.getCriteries()) {
                convertConditionParam(params, criteria);
            }
        }
        return params;
    }

    private void convertConditionParam(List params, {{metadata.exampleClazzSimpleName}}.Criteria criteria){
        String column = criteria.getColumn();
        {{#metadata.columnMetadataList}}
        if ("{{originColumnName}}".equals(column) || "{{columnName}}".equals(column) || "{{fieldName}}".equals(column)) {
            if (criteria.getValue() != null) {
                Object value = defaultTypeHandler.encode{{firstUpFieldName}}(({{fullJavaType}}) criteria.getValue());
                params.add(value);
                return;
            }
            if (criteria.getSecondValue() != null) {
                Object value = defaultTypeHandler.encode{{firstUpFieldName}}(({{fullJavaType}}) criteria.getSecondValue());
                params.add(value);
                return;
            }
            if (criteria.getListValue() != null) {
                List values = defaultTypeHandler.encode{{firstUpFieldName}}List((List<{{fullJavaType}}>) criteria.getListValue());
                params.addAll(values);
                return;
            }
        }
        {{/metadata.columnMetadataList}}

    }
    protected String toConditionSql({{metadata.exampleClazzName}} example){
        return toConditionSql(example, false);
    }
    protected String toConditionSql({{metadata.exampleClazzName}} example, boolean isCount) {
        List<List<{{metadata.exampleClazzName}}.Criteria>> orConditions = example.getOrConditions();

        String orderByClause = example.getOrderByClause();
        List<Integer> limit = example.getLimit();
        StringBuilder sql = new StringBuilder(32);
        if (orConditions != null && !orConditions.isEmpty()) {
            sql.append(" where ");
            orConditions.add(example.getCriteries());
            for (int j = 0; j < orConditions.size(); j++) {
                if (j > 0 && j < orConditions.size() ) {
                    sql.append(" or ");
                }
                List<{{metadata.exampleClazzName}}.Criteria> criteris = orConditions.get(j);
                sql.append("(");
                for (int i = 0; i < criteris.size(); i++) {
                    if (i > 0 && i < criteris.size() ) {
                        sql.append(" and ");
                    }
                    {{metadata.exampleClazzName}}.Criteria criteria = criteris.get(i);
                    if (criteria.getValue() != null && criteria.getSecondValue() != null) {
                        sql.append(criteria.getColumn())
                                .append(criteria.getCondition())
                                .append("? and ?");
                    } else if (criteria.getValue() != null) {
                        sql.append(criteria.getColumn())
                                .append(criteria.getCondition())
                                .append("?");
                    } else if (criteria.getListValue() != null) {
                        sql.append(criteria.getColumn())
                                .append(criteria.getCondition())
                                .append(appendPlaceholder(criteria.getListValue().size()));
                    } else if (criteria.getCondition() != null) {
                      sql.append(criteria.getCondition());
                    }
                }
                sql.append(")");
            }

        } else if (!example.getCriteries().isEmpty()) {
            sql.append(" where ");
            List<{{metadata.exampleClazzName}}.Criteria> criteris = example.getCriteries();
            for (int i = 0; i < criteris.size(); i++) {
                if (i > 0 && i < criteris.size() ) {
                    sql.append(" and ");
                }
                {{metadata.exampleClazzName}}.Criteria criteria = criteris.get(i);
                if (criteria.getValue() != null && criteria.getSecondValue() != null) {
                    sql.append(criteria.getColumn())
                            .append(criteria.getCondition())
                            .append("? and ?");
                } else if (criteria.getValue() != null) {
                    sql.append(criteria.getColumn())
                            .append(criteria.getCondition())
                            .append("?");
                } else if (criteria.getListValue() != null) {
                    sql.append(criteria.getColumn())
                            .append(criteria.getCondition())
                            .append(appendPlaceholder(criteria.getListValue().size()));
                } else if (criteria.getCondition() != null) {
                    sql.append(criteria.getCondition());
                }
            }
        }

        if (example.getGroupBy() != null && !example.getGroupBy().isEmpty()) {
            sql.append(" group by ");
            sql.append(String.join(", ", example.getGroupBy()));
        }

        if (example.getHaving() != null && !example.getHaving().isEmpty()) {
            sql.append(" having ");
            sql.append(String.join(" and ", example.getHaving()));
        }

        if (orderByClause != null && !orderByClause.trim().isEmpty()) {
            sql.append(" order by ");
            sql.append(orderByClause);
        }

        if (!isCount && limit != null && !limit.isEmpty()) {
            sql.append(" limit ");
            if (limit.size() == 1) {
                sql.append(limit.get(0));
            }
            if (limit.size() == 2) {
                {{#metadata.none}}
                sql.append(limit.get(0));
                sql.append(" , ");
                sql.append(limit.get(1));
                {{/metadata.none}}
                {{#metadata.mysql}}
                sql.append(limit.get(0));
                sql.append(" , ");
                sql.append(limit.get(1));
                {{/metadata.mysql}}
                {{#metadata.postgres}}
                sql.append(limit.get(0));
                sql.append(" offset ");
                sql.append(limit.get(1));
                {{/metadata.postgres}}
                {{#metadata.mssql}}
                sql.append(" offset ")
                sql.append(limit.get(0));
                sql.append(" rows FETCH NEXT ");
                sql.append(limit.get(1));
                sql.append(" ROWS ONLY");
                {{/metadata.mssql}}
                {{#metadata.oracle}}
                sql.append(" offset ")
                sql.append(limit.get(0));
                sql.append(" rows FETCH NEXT ");
                sql.append(limit.get(1));
                sql.append(" ROWS ONLY");
                {{/metadata.oracle}}
            }
        }
        return sql.toString();
    }

    protected String toSelectByExampleSql({{metadata.exampleClazzName}} example) {
        boolean distinct = example.isDistinct();
        List<String> selectColumns = example.getColumns();
        String table = tableName;
        StringBuilder sql = new StringBuilder(32 + columnsStr.length());

        sql.append("select ");
        if (distinct) {
            sql.append("distinct ");
        }

        if (selectColumns != null && selectColumns.isEmpty()) {
            sql.append(String.join(", ", selectColumns));
        } else {
            sql.append(columnsStr);
        }
        sql.append(" from ");
        sql.append(table);
        sql.append(toConditionSql(example));
        if(example.getForUpdate()!=null && example.getForUpdate()){
            sql.append(" for update");
        }
        return sql.toString();
    }

    protected String toCountByExampleSql({{metadata.exampleClazzName}} example) {
        boolean distinct = example.isDistinct();
        List<String> selectColumns = example.getColumns();
        String table = tableName;
        StringBuilder sql = new StringBuilder();

        sql.append("select ");
        if (selectColumns != null && !selectColumns.isEmpty()) {
            sql.append("count(");
            if (distinct) {
                sql.append("distinct ");
            }
            sql.append(String.join(", ", selectColumns));
            sql.append(")");
        } else {
            sql.append("count(*)");
        }
        sql.append(" from ");
        sql.append(table);
        sql.append(toConditionSql(example,true));
        return sql.toString();
    }

    protected Connection getConnection() {
        return getConnection(false);
    }

    {{#metadata.useSpring}}
    protected Connection getConnection(boolean isSelect) {
        if ({{{metadata.readOnly}}}){
            if(!isSelect){
                throw new RuntimeException("ReadOnly connection is not allowed to execute update operation");
            }
            try{
                String name;
                if(!slaveDataSources.isEmpty()){
                    if(slaveDataSources.size() == 1){
                        name = slaveDataSources.get(0);
                    } else {
                        name = slaveDataSources.get((int) (counter.incrementAndGet() % slaveDataSources.size()));
                    }
                    if(log.isDebugEnabled()){
                        log.debug("Use slave dataSource {}",name);
                    }
                    Connection connection =  dataSourceMap.get(name).getConnection();
                    connection.setReadOnly(true);
                    return connection;
                }
                Connection connection =  this.getDataSource().getConnection();
                connection.setReadOnly(true);
                return connection;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if(isSelect && !slaveDataSources.isEmpty() && !isActualTransactionActive()){
            try {
               String name;
               if(slaveDataSources.size() == 1){
                   name = slaveDataSources.get(0);
               } else {
                   name = slaveDataSources.get((int) (counter.incrementAndGet() % slaveDataSources.size()));
               }
               if(log.isDebugEnabled()){
                   log.debug("Use slave dataSource {}",name);
               }
               Connection connection =  dataSourceMap.get(name).getConnection();
               connection.setReadOnly(true);
               return connection;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        Connection connection = org.springframework.jdbc.datasource.DataSourceUtils.getConnection(this.getDataSource());
        try{
            if (connection.isClosed()){
                return this.getDataSource().getConnection();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
    {{/metadata.useSpring}}

    {{^metadata.useSpring}}@org.springframework.beans.factory.annotation.Autowired
    protected Connection getConnection(boolean isSelect) {
        try {
            return this.getDataSource().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    {{/metadata.useSpring}}
    {{#metadata.useSpring}}@org.springframework.beans.factory.annotation.Autowired{{/metadata.useSpring}}
    {{#metadata.dataSource}}@org.springframework.beans.factory.annotation.Qualifier("{{metadata.dataSource}}"){{/metadata.dataSource}}
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    {{^metadata.shard}}
    {{#metadata.useSpring}}@org.springframework.beans.factory.annotation.Autowired{{/metadata.useSpring}}
    public void setSlaveDataSourceMap(Map<String, DataSource> slaveDataSourceMap) {
        this.dataSourceMap = slaveDataSourceMap;
        {{#metadata.slaveDataSources}}
        if(slaveDataSourceMap.get("{{.}}") != null){
            slaveDataSources.add("{{.}}");
        }
        {{/metadata.slaveDataSources}}
    }
    {{/metadata.shard}}

    public DataSource getDataSource() {
        return dataSource;
    }

    {{#metadata.useSpring}}
    protected boolean isActualTransactionActive(){
        return org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive();
    }
    {{/metadata.useSpring}}
    {{^metadata.useSpring}}
    protected boolean isActualTransactionActive(){
        return false;
    }
    {{/metadata.useSpring}}


    public {{metadata.typeHandlerClazzName}} getDefaultTypeHandler() {
        return defaultTypeHandler;
    }

    {{#metadata.useSpring}}@org.springframework.beans.factory.annotation.Autowired(required = false){{/metadata.useSpring}}
    public void setDefaultTypeHandler({{metadata.typeHandlerClazzName}} defaultTypeHandler) {
        this.defaultTypeHandler = defaultTypeHandler;
    }

    protected org.slf4j.Logger getLogger(){
        return log;
    }

    protected String paramsToString(Object[] params) {
        return paramsToString(Arrays.asList(params));
    }

    protected String paramsToString(List params) {
        if(params == null || params.isEmpty()){
            return "[]";
        }
        StringBuilder sb = new StringBuilder()
                .append("[");
        String format = "yyyy-MM-dd HH:mm:ss.SSS";
        java.text.SimpleDateFormat sf = new java.text.SimpleDateFormat(format);
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);

        for (Object param : params) {
            if (param instanceof java.util.Date) {
                param = sf.format((java.util.Date) param);
            } else if (param instanceof java.time.temporal.TemporalAccessor) {
                param = formatter.format((java.time.temporal.TemporalAccessor) param);
            } else if(param instanceof byte[]){
                param = Arrays.toString((byte[]) param);
            } else {
                param = String.valueOf(param);
            }

            sb.append(param).append(", ");
        }
        return sb.substring(0, sb.length() - 2) + "]";
    }

    public io.github.simple.jdbc.processor.PageInfo<{{metadata.domainClazzName}}> pageByExample({{metadata.exampleClazzName}} q) {
        Integer page = q.getPage();
        Integer size = q.getSize();
        List<{{metadata.domainClazzName}}> ts = selectByExample(q);
        long total = ts.size();
        if (page == 1 && total < size) {
            total = ts.size();
        } else {
            total = countByExample(q);
        }
        return new io.github.simple.jdbc.processor.PageInfo<>(page, size, total, ts);
    }

    protected String in(int size){
        return " in " + appendPlaceholder(size);
    }

    protected String notIn(int size){
        return " not in " + appendPlaceholder(size);
    }

    protected List<String> getColumns(){
        return new ArrayList<>(this.columns);
    }

    protected String getColumnsStr() {
        return this.columnsStr;
    }
}
