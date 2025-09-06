package {{metadata.packageName}};


import javax.sql.DataSource;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public abstract class {{metadata.repositoryClazzSimpleName}} {{#metadata.extendsSimpleJdbcRepository}}implements io.github.simple.jdbc.processor.SimpleJdbcRepository<{{metadata.domainClazzSimpleName}}, {{metadata.primaryMetadata.javaType}}, {{metadata.exampleClazzSimpleName}}> {{/metadata.extendsSimpleJdbcRepository}}{

    protected final org.slf4j.Logger log;

    private final java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong();

    private DataSource dataSource;

    private List<String> slaveDataSources  = new ArrayList<>();

    private Map<String, DataSource> dataSourceMap = new HashMap<>();

    private {{metadata.typeHandlerClazzName}} defaultTypeHandler = new {{metadata.typeHandlerClazzName}}();

    private String tableName = "{{metadata.tableName}}";

    private String columnsStr = "{{metadata.columns}}";

    private List<String> columns = Arrays.asList(columnsStr.split(", "));

    private String primaryKeyStr = "{{metadata.primaryMetadata.columnName}}";

    private Class<{{metadata.primaryMetadata.javaType}}> primaryKeyType = {{metadata.primaryMetadata.javaType}}.class;

    private String primaryKeyCondition;

    private String primaryKeyInCondition;

    private String selectByPrimaryKeySql;

    private String selectByPrimaryKeyForUpdateSql;

    private String selectByPrimaryKeysSql;

    private String updatePrefix;

    private String updateSuffix;

    private String updateByExamplePrefix;

    private String updateByPrimaryKeySql;

    private String deletePrefix;

    private String deleteByPrimaryKeySql;

    private String deleteByPrimaryKeysSql;

    private String insertSqlPrefix;

    private String insertSql;


    public {{metadata.repositoryClazzSimpleName}}(){
        this.log = org.slf4j.LoggerFactory.getLogger(this.getClass());
        init();
    }
{{#metadata.shard}}
    public {{metadata.repositoryClazzSimpleName}}(Class<?> logClazz, String tableName, DataSource dataSource, Map<String, DataSource> dataSourceMap, {{metadata.typeHandlerClazzName}} defaultTypeHandler){
        this.log = org.slf4j.LoggerFactory.getLogger(logClazz);
        this.tableName = tableName;
        this.dataSource = dataSource;
        this.defaultTypeHandler = defaultTypeHandler;
        this.dataSourceMap = dataSourceMap;
        this.slaveDataSources = new ArrayList<>(dataSourceMap.keySet());
        init();
    }
{{/metadata.shard}}

    private void init() {
        this.primaryKeyCondition = " where " + primaryKeyStr + " = ?";
        this.primaryKeyInCondition = " where " + primaryKeyStr + " in ";
        this.selectByPrimaryKeySql = "select " + columnsStr + " from " + tableName + primaryKeyCondition;
        this.selectByPrimaryKeyForUpdateSql = "select " + columnsStr + " from " + tableName + primaryKeyCondition + " for update";
        this.selectByPrimaryKeysSql = "select " + columnsStr + " from " + tableName + primaryKeyInCondition;
        this.updatePrefix = "update " + tableName;
        this.updateSuffix = " set " + String.join(" = ?, ", columnsStr.split(",")) + " = ?";
        this.updateByExamplePrefix = updatePrefix + updateSuffix;
        this.updateByPrimaryKeySql = updatePrefix + updateSuffix + primaryKeyCondition;
        this.deletePrefix = "delete from " + tableName;
        this.deleteByPrimaryKeySql = deletePrefix + primaryKeyCondition;
        this.deleteByPrimaryKeysSql = deletePrefix + primaryKeyInCondition;
        this.insertSqlPrefix = "insert into " + tableName + " (" + columnsStr + ") values ";
        this.insertSql = insertSqlPrefix + appendPlaceholder(columns.size());
    }


    public {{metadata.domainClazzName}} selectByPrimaryKey({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        return selectOne(selectByPrimaryKeySql, Collections.singletonList({{metadata.primaryMetadata.fieldName}}), r-> getDefaultTypeHandler().handle(r));
    }

    public {{metadata.domainClazzName}} selectByPrimaryKeyForUpdate({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        return selectOne(selectByPrimaryKeyForUpdateSql, Collections.singletonList({{metadata.primaryMetadata.fieldName}}), r-> getDefaultTypeHandler().handle(r));
    }

    public List<{{metadata.domainClazzName}}> selectByPrimaryKeys(List<{{metadata.primaryMetadata.javaType}}> {{metadata.primaryMetadata.fieldName}}s) {
        String sql = selectByPrimaryKeysSql + appendPlaceholder({{metadata.primaryMetadata.fieldName}}s.size());
        return selectList(sql, {{metadata.primaryMetadata.fieldName}}s, r-> getDefaultTypeHandler().handle(r));
    }

    public int updateByPrimaryKey({{metadata.domainClazzName}} t) {
        getDefaultTypeHandler().preUpdate(t);
        List<Object> params = new ArrayList<>(columns.size());
        getDefaultTypeHandler().encode(params, t);
        params.add(t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        int affect = update(updateByPrimaryKeySql, params);
        getDefaultTypeHandler().afterUpdate(t);
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
        getDefaultTypeHandler().preUpdate(t);
        StringBuilder prefix = new StringBuilder()
                .append("update ")
                .append(getTableName())
                .append(" set ");
        List<Object> params = new ArrayList<>(columns.size());
        getDefaultTypeHandler().encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            prefix.append("{{columnName}} = ?, ");
        }
        {{/metadata.columnMetadataList}}
        params.add(t.get{{metadata.primaryMetadata.firstUpFieldName}}());
        String sql = prefix.substring(0, prefix.length() - 2) + primaryKeyCondition;
        int affect = update(sql, params);
        getDefaultTypeHandler().afterUpdate(t);
        return affect;
    }

    public int deleteByPrimaryKey({{metadata.primaryMetadata.javaType}} {{metadata.primaryMetadata.fieldName}}) {
        List<Object> params = new ArrayList<>();
        params.add({{metadata.primaryMetadata.fieldName}});
        return delete(deleteByPrimaryKeySql, params);
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
            return selectList(sql, getConditionValues(example), r-> getDefaultTypeHandler().handle(r));
        }
        return selectList(sql, getConditionValues(example), rs -> getDefaultTypeHandler().handle(rs, columns));
    }


    public long countByExample({{metadata.exampleClazzName}} example) {
        String sql = toCountByExampleSql(example);
        return selectOne(sql, getConditionValues(example), rs -> rs.getLong(1));
    }


    public int updateByExample({{metadata.domainClazzName}} t, {{metadata.exampleClazzName}} example) {
        List<Object> params;
        if (example.getUpdateSetValues() != null) {
            params = new ArrayList<>(example.getUpdateSetValues());
        } else {
            params = new ArrayList<>(columns.size());
        }

        getDefaultTypeHandler().encode(params, t);
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
        StringBuilder prefix = new StringBuilder()
                .append("update ")
                .append(getTableName())
                .append(" set ");

        List<Object> params;
        if (example.getUpdateSetValues() != null) {
            params = new ArrayList<>(example.getUpdateSetValues());
        } else {
            params = new ArrayList<>(columns.size());
        }

        if (example.getUpdateExpression() != null) {
            prefix.append(String.join(", ", example.getUpdateExpression()));
        }

        getDefaultTypeHandler().encodeSelective(params, t);

        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            prefix.append("{{columnName}} = ?, ");
        }
        {{/metadata.columnMetadataList}}
        params.addAll(getConditionValues(example));
        String sql = prefix.substring(0, prefix.length() - 2) + toConditionSql(example);
        return update(sql, params);
    }

    public int updateByExampleSelective({{metadata.exampleClazzName}} example) {
        StringBuilder prefix = new StringBuilder()
                .append("update ")
                .append(getTableName())
                .append(" set ");

        List<Object> params;
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
        Map<String,List<Object[]>> m = new LinkedHashMap<>();
        for({{metadata.exampleClazzName}} example: examples){
            StringBuilder prefix = new StringBuilder()
                    .append("update ")
                    .append(getTableName())
                    .append(" set ");
            List<Object> params;
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
        getDefaultTypeHandler().preInsert(t);
        List<Object> params = new ArrayList<>(columns.size());
        getDefaultTypeHandler().encode(params, t);
        {{#metadata.primaryMetadata}}
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(insertSql, params);
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata}}
        getDefaultTypeHandler().afterInsert(t);
    }

    public void insertBatch(List<{{metadata.domainClazzName}}> ts) {
        if (ts == null || ts.isEmpty()) {
            throw new IllegalArgumentException("ts is null or empty");
        }
        List<Object> params = new ArrayList<>(columns.size() * ts.size());
        StringBuilder sql = new StringBuilder(insertSqlPrefix);

        for ({{metadata.domainClazzName}} t : ts) {
            getDefaultTypeHandler().preInsert(t);
            getDefaultTypeHandler().encode(params, t);
            sql.append(appendPlaceholder(columns.size()))
                    .append(", ");
        }
        {{#metadata.primaryMetadata}}
        List<{{metadata.primaryMetadata.javaType}}> primaryKeys = insertBatch(sql.substring(0, sql.length() - 2), params);
        int index = 0;
        for (int i = 0; i < ts.size(); i++) {
            {{metadata.domainClazzName}} t = ts.get(i);
            if(t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null){
                ts.get(index).set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKeys.get(index));
                index++;
            }
            getDefaultTypeHandler().afterInsert(t);
        }
        {{/metadata.primaryMetadata}}
    }

    public void insertSelective({{metadata.domainClazzName}} t) {
        getDefaultTypeHandler().preInsert(t);
        List<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("insert into ")
                .append(getTableName())
                .append(" (");
        getDefaultTypeHandler().encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}


        String sql = prefix.substring(0, prefix.length() - 2) + ") values " + appendPlaceholder(params.size());
        {{#metadata.primaryMetadata}}
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata}}
        getDefaultTypeHandler().afterInsert(t);
    }


    public void upsertSelective({{metadata.domainClazzName}} t) {
        getDefaultTypeHandler().preInsert(t);
        List<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("replace into ")
                .append(getTableName())
                .append(" (");

        getDefaultTypeHandler().encodeSelective(params, t);

        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}


        String sql = prefix.substring(0, prefix.length() - 2) + ") values " + appendPlaceholder(params.size());
        {{#metadata.primaryMetadata}}
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata}}
        getDefaultTypeHandler().afterInsert(t);
    }


    public {{metadata.primaryMetadata.javaType}} insertIgnoreSelective({{metadata.domainClazzName}} t) {
        getDefaultTypeHandler().preInsert(t);
        List<Object> params = new ArrayList<>(columns.size());

        StringBuilder prefix = new StringBuilder()
                .append("insert ignore into ")
                .append(getTableName())
                .append(" (");

        getDefaultTypeHandler().encodeSelective(params, t);
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            prefix.append("{{columnName}}, ");
        }
        {{/metadata.columnMetadataList}}


        String sql = prefix.substring(0, prefix.length() - 2) + ") values " + appendPlaceholder(params.size());
        {{#metadata.primaryMetadata}}
        {{metadata.primaryMetadata.javaType}} primaryKey = insert(sql, params);
        if (t.get{{metadata.primaryMetadata.firstUpFieldName}}() == null && primaryKey > 0) {
            t.set{{metadata.primaryMetadata.firstUpFieldName}}(primaryKey);
        }
        {{/metadata.primaryMetadata}}
        getDefaultTypeHandler().afterInsert(t);
        return primaryKey;
    }


    public int deleteByExample({{metadata.exampleClazzName}} example) {
        String sql = deletePrefix + toConditionSql(example);
        return delete(sql, getConditionValues(example));
    }

    protected <T> List<T> selectList(String sql, List params, Handler<T> handler) {
        getDefaultTypeHandler().postSelect(sql, params);
        List<T> list = new ArrayList<>();
        Connection connection = getConnection(true);
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            getDefaultTypeHandler().setParameters(statement, params);
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
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Total:      {}", list.size());
        }
        return list;
    }


    public void consumeByExample({{metadata.exampleClazzName}} example, Consumer<{{metadata.domainClazzName}}> consume) {
        String sql = toSelectByExampleSql(example);
        List<String> columns = example.getColumns();
        if (columns == null || columns.isEmpty()) {
            consume(sql, getConditionValues(example), r-> getDefaultTypeHandler().handle(r),consume);
            return;
        }
        consume(sql, getConditionValues(example), rs -> getDefaultTypeHandler().handle(rs, columns),consume);
    }


    protected <T> void consume(String sql, List params, Handler<T> handler, Consumer<T> consumer) {
        getDefaultTypeHandler().postSelect(sql, params);
        List<T> list = new ArrayList<>();
        Connection connection = getConnection(true);
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
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
            getDefaultTypeHandler().setParameters(statement, params);
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
                close(connection);
            }
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Total:      {}", total);
        }
    }


    public List<{{metadata.domainClazzName}}> selectByPrimaryKeysWithSorted(List<{{metadata.primaryMetadata.javaType}}> ids) {
        List<{{metadata.domainClazzName}}> ts = selectByPrimaryKeys(ids);
        if (!ts.isEmpty()) {
            Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> m = ts.stream()
                    .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, Function.identity()));
            return ids.stream()
                    .map(m::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return ts;
    }

    public Map<{{metadata.primaryMetadata.javaType}}, {{metadata.domainClazzName}}> mapById(List<{{metadata.primaryMetadata.javaType}}> ids) {
        return selectByPrimaryKeys(ids)
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap({{metadata.domainClazzName}}::get{{metadata.primaryMetadata.firstUpFieldName}}, Function.identity()));
    }


    protected <T> T selectOne(String sql, List params, Handler<T> handler) {
        getDefaultTypeHandler().postSelect(sql, params);
        Connection connection = getConnection(true);
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            getDefaultTypeHandler().setParameters(statement, params);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Total:      {}", 1);
                    }
                    return handler.handle(resultSet);
                } else {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Total:      {}", 0);
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
        getDefaultTypeHandler().postUpdate(sql , params);
        Connection connection = getConnection();

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            getDefaultTypeHandler().setParameters(statement, params);
            int affect = statement.executeUpdate();
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Total:      {}", affect);
            }
            return affect;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
    }

    protected int[] updateBatch(String sql, List<Object[]> batchParams) {
        getDefaultTypeHandler().postUpdate(sql , batchParams);
        Connection connection = getConnection();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Object[] batchParam : batchParams) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Preparing:  {}", sql);
                    getLogger().debug("Parameters: {}", Arrays.toString(batchParam));
                }
                getDefaultTypeHandler().setParameters(statement, batchParam);
                statement.addBatch();
            }
            int[] affects = statement.executeBatch();
            if (getLogger().isDebugEnabled()) {
                for (int affect : affects) {
                    getLogger().debug("Total:      {}", affect);
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
        getDefaultTypeHandler().postDelete(sql , params);
        Connection connection = getConnection();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            getDefaultTypeHandler().setParameters(statement, params);
            int affect = statement.executeUpdate();
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Total:      {}", affect);
            }
            return affect;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
    }

    protected {{metadata.primaryMetadata.javaType}} insert(String sql, List params) {
        getDefaultTypeHandler().postInsert(sql , params);
        Connection connection = getConnection();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql{{#metadata.primaryMetadata.useGeneratedKeys}}, Statement.RETURN_GENERATED_KEYS{{/metadata.primaryMetadata.useGeneratedKeys}})) {
            getDefaultTypeHandler().setParameters(statement, params);
            int affect = statement.executeUpdate();
            {{#metadata.primaryMetadata.useGeneratedKeys}}
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getObject(1, {{metadata.primaryMetadata.javaType}}.class);
            }
            {{/metadata.primaryMetadata.useGeneratedKeys}}
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Total:      {}", affect);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        return {{metadata.primaryMetadata.defaultValue}};
    }


    protected List<{{metadata.primaryMetadata.javaType}}> insertBatch(String sql, List<Object> params) {
        getDefaultTypeHandler().postInsert(sql , params);
        Connection connection = getConnection();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Preparing:  {}", sql);
            getLogger().debug("Parameters: {}", params);
        }
        List<{{metadata.primaryMetadata.javaType}}> ids = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql{{#metadata.primaryMetadata.useGeneratedKeys}}, Statement.RETURN_GENERATED_KEYS{{/metadata.primaryMetadata.useGeneratedKeys}})) {
            getDefaultTypeHandler().setParameters(statement, params);
            statement.executeUpdate();
            {{#metadata.primaryMetadata.useGeneratedKeys}}
            ResultSet generatedKeys = statement.getGeneratedKeys();
            while (generatedKeys.next()) {
                {{metadata.primaryMetadata.javaType}} primaryKey = generatedKeys.getObject(1, {{metadata.primaryMetadata.javaType}}.class);
                ids.add(primaryKey);
            }
            {{/metadata.primaryMetadata.useGeneratedKeys}}
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeCheckTx(connection);
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Total:      {}", ids.size());
        }
        return ids;
    }

    public <T> List<T> aggregate({{metadata.exampleClazzSimpleName}} example, Handler<T> handler) {
        StringBuilder sb = new StringBuilder("select ")
                .append(String.join(", ", example.getAggregates()))
                .append(" from ")
                .append(getTableName())
                .append(toConditionSql(example));

        return selectList(sb.toString(), getConditionValues(example), handler);
    }

    public <T> T aggregateOne({{metadata.exampleClazzSimpleName}} example, Handler<T> handler) {
        StringBuilder sb = new StringBuilder("select ")
                .append(String.join(", ", example.getAggregates()))
                .append(" from ")
                .append(getTableName())
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
        StringBuilder sql = new StringBuilder();
        sql.append("(");
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                sql.append("?");
            } else {
                sql.append(", ?");
            }
        }
        sql.append(")");
        return sql.toString();
    }

    protected List getConditionValues({{metadata.exampleClazzName}} example) {
        List<Object> params = new ArrayList<>();
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
                Object value = getDefaultTypeHandler().encode{{firstUpFieldName}}(({{javaType}}) criteria.getValue());
                params.add(value);
                return;
            }
            if (criteria.getSecondValue() != null) {
                Object value = getDefaultTypeHandler().encode{{firstUpFieldName}}(({{javaType}}) criteria.getSecondValue());
                params.add(value);
                return;
            }
            if (criteria.getListValue() != null) {
                List values = getDefaultTypeHandler().encode{{firstUpFieldName}}List((List<{{javaType}}>) criteria.getListValue());
                params.addAll(values);
                return;
            }
        }
        {{/metadata.columnMetadataList}}

    }

    protected String toConditionSql({{metadata.exampleClazzName}} example) {
        List<List<{{metadata.exampleClazzName}}.Criteria>> orConditions = example.getOrConditions();

        String orderByClause = example.getOrderByClause();
        List<Integer> limit = example.getLimit();
        StringBuilder sql = new StringBuilder();
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

        if (orderByClause != null && !orderByClause.trim().isEmpty()) {
            sql.append(" order by ");
            sql.append(orderByClause);
        }

        if (example.getGroupBy() != null && !example.getGroupBy().isEmpty()) {
            sql.append(" group by ");
            sql.append(String.join(", ", example.getGroupBy()));
        }

        if (example.getHaving() != null && !example.getHaving().isEmpty()) {
            sql.append(" having ");
            sql.append(String.join(" and ", example.getHaving()));
        }


        if (limit != null && !limit.isEmpty()) {
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
        String table = getTableName();
        StringBuilder sql = new StringBuilder();

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
        return sql.toString();
    }

    protected String toCountByExampleSql({{metadata.exampleClazzName}} example) {
        boolean distinct = example.isDistinct();
        List<String> selectColumns = example.getColumns();
        String table = getTableName();
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
        sql.append(toConditionSql(example));
        return sql.toString();
    }

    protected String getTableName() {
        return tableName;
    }

    protected Connection getConnection() {
        return getConnection(false);
    }

    {{#metadata.useSpring}}
    protected Connection getConnection(boolean isSelect) {
        if(isSelect && !slaveDataSources.isEmpty() && !isActualTransactionActive()){
            try {
               String name;
               if(slaveDataSources.size() == 1){
                   name = slaveDataSources.get(0);
               } else {
                   name = slaveDataSources.get((int) (counter.incrementAndGet() % slaveDataSources.size()));
               }
               if(getLogger().isDebugEnabled()){
                   getLogger().debug("Use slave dataSource {}",name);
               }
               Connection connection =  dataSourceMap.get(name).getConnection();
               connection.setReadOnly(true);
               return connection;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return org.springframework.jdbc.datasource.DataSourceUtils.getConnection(this.getDataSource());
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
}
