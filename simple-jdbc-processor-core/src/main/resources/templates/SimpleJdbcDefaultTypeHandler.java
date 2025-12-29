package {{metadata.packageName}};


import javax.sql.DataSource;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class {{metadata.typeHandlerClazzSimpleName}} {


    public void preUpdate({{metadata.domainClazzName}} t) {

    }

    public void postUpdate(String sql, List params) {

    }

    public void postUpdate(String sql, Object[] params) {

    }

    public void afterUpdate({{metadata.domainClazzName}} t) {

    }

    public void preInsert({{metadata.domainClazzName}} t) {

    }

    public void postInsert(String sql, List params) {

    }

    public void afterInsert({{metadata.domainClazzName}} t) {

    }

    public void postDelete(String sql, List params) {

    }

    public void preSelect(String sql, List params) {

    }

    public void postSelect(String sql, List params) {

    }

    public void encode(List params, {{metadata.domainClazzName}} t) {
        {{#metadata.columnMetadataList}}
        params.add(encode{{firstUpFieldName}}(t.get{{firstUpFieldName}}()));
        {{/metadata.columnMetadataList}}
    }

    public void encodeSelective(List params, {{metadata.domainClazzName}} t) {
        {{#metadata.columnMetadataList}}
        if (t.get{{firstUpFieldName}}() != null) {
            params.add(encode{{firstUpFieldName}}(t.get{{firstUpFieldName}}()));
        }
        {{/metadata.columnMetadataList}}
    }

    public void setParameters(PreparedStatement ps, List params) throws SQLException {
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                setParameter(ps, i + 1, param);
            }
        }
    }

    public void setParameters(PreparedStatement ps, Object[] params) throws SQLException {
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                setParameter(ps, i + 1, params[i]);
            }
        }
    }

    public void setParameter(PreparedStatement ps, int paramIndex, Object param) throws SQLException {
        if (param == null) {
            ps.setNull(paramIndex, JDBCType.VARCHAR.getVendorTypeNumber());
            return;
        }

        if (param instanceof Boolean) {
            ps.setBoolean(paramIndex, (Boolean) param);
            return;
        }

        if (param instanceof CharSequence) {
            String str = param.toString();
            if (str.length() > 40000) {
                ps.setClob(paramIndex, new java.io.StringReader(str));
            } else {
                ps.setString(paramIndex, str);
            }
            return;
        }

        if (param instanceof Number) {
            if (param instanceof Byte) {
                byte b = (Byte) param;
                ps.setByte(paramIndex, b);
            } else if (param instanceof Short) {
                short n = (Short) param;
                ps.setShort(paramIndex, n);
            } else if (param instanceof Integer) {
                int n = (Integer) param;
                ps.setInt(paramIndex, n);
            } else if (param instanceof Long) {
                long n = (Long) param;
                ps.setLong(paramIndex, n);
            } else if (param instanceof Float) {
                float n = (Float) param;
                ps.setFloat(paramIndex, n);
            } else if (param instanceof Double) {
                double n = (Double) param;
                ps.setDouble(paramIndex, n);
            } else if (param instanceof BigDecimal) {
                ps.setBigDecimal(paramIndex, (BigDecimal) param);
            } else if (param instanceof BigInteger) {
                ps.setBigDecimal(paramIndex, new BigDecimal((BigInteger) param));
            } else {
                ps.setObject(paramIndex, param);
            }
            return;
        }


        if (param instanceof java.util.Date) {
            if (param instanceof java.sql.Date) {
                ps.setDate(paramIndex, (java.sql.Date) param);
            } else if (param instanceof java.sql.Time) {
                ps.setTime(paramIndex, (java.sql.Time) param);
            } else {
                ps.setTimestamp(paramIndex, new java.sql.Timestamp(((java.util.Date) param).getTime()));
            }
            return;
        }

        if (param instanceof java.util.Calendar){
            ps.setTimestamp(paramIndex, java.sql.Timestamp.from(((java.util.Calendar) param).toInstant()));
            return;
        }

        if (param instanceof UUID) {
            ps.setString(paramIndex, ((UUID) param).toString());
            return;
        }

        if (param instanceof byte[]) {
            ps.setBytes(paramIndex, (byte[]) param);
            return;
        }

        if (param instanceof Enum) {
            ps.setString(paramIndex, ((Enum<?>) param).name());
            return;
        }

        ps.setObject(paramIndex, param);
    }


    public {{metadata.domainClazzName}} handle(ResultSet rs, List<String> columns) throws SQLException {
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            {{#metadata.columnMetadataList}}
            if ("{{originColumnName}}".equals(column) || "{{columnName}}".equals(column) || "{{fieldName}}".equals(column)) {
                decode{{firstUpFieldName}}(rs, t, "{{originColumnName}}", i + 1, {{sqlJavaType}}.class);
                continue;
            }
            {{/metadata.columnMetadataList}}
        }
        return t;
    }

    public {{metadata.domainClazzName}} handle(ResultSet rs) throws SQLException {
        int index = 1;
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        {{#metadata.columnMetadataList}}
        decode{{firstUpFieldName}}(rs, t, "{{originColumnName}}", index++, {{sqlJavaType}}.class);
        {{/metadata.columnMetadataList}}
        return t;
    }

    {{#metadata.columnMetadataList}}
    public void decode{{firstUpFieldName}}(ResultSet resultSet, {{metadata.domainClazzName}} t, String column, int index, Class<{{sqlJavaType}}> targetType) throws SQLException {
        if (index > 0) {
            {{#isEnums}}
            String value = resultSet.getString(index);
            if(value == null){
                return;
            }
            t.set{{firstUpFieldName}}(Enum.valueOf(targetType, value));
            {{/isEnums}}
            {{^isEnums}}
            {{#knowResultSetType}}
            {{javaType}} value = resultSet.{{resultSetGetMethodName}}(index);
            {{/knowResultSetType}}
            {{^knowResultSetType}}
            {{javaType}} value = resultSet.getObject(index, targetType);
            {{/knowResultSetType}}
            if(value == null){
                return;
            }
            t.set{{firstUpFieldName}}(value);
            {{/isEnums}}
        } else {
            {{#isEnums}}
            String value = resultSet.getString(column);
            if(value == null){
                return;
            }
            t.set{{firstUpFieldName}}(Enum.valueOf(targetType, value));
            {{/isEnums}}
            {{^isEnums}}
            {{#knowResultSetType}}
            {{javaType}} value = resultSet.{{resultSetGetMethodName}}(column);
            {{/knowResultSetType}}
            {{^knowResultSetType}}
            {{javaType}} value = resultSet.getObject(column, targetType);
            {{/knowResultSetType}}
            if(value == null){
                return;
            }
            t.set{{firstUpFieldName}}(value);
            {{/isEnums}}
        }
    }

    public Object encode{{firstUpFieldName}}({{javaType}} value) {
        {{^isEnums}}return value;{{/isEnums}}{{#isEnums}}return value == null ? null: String.valueOf(value);{{/isEnums}}
    }

    public List encode{{firstUpFieldName}}List(List<{{javaType}}> values) {
        {{^isEnums}}return values;{{/isEnums}}
        {{#isEnums}}return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.toList());{{/isEnums}}
    }

    {{/metadata.columnMetadataList}}

    {{#metadata.primaryMetadata}}
    public void generatePrimaryKey({{metadata.domainClazzName}} t) {
    }

    public void batchGeneratePrimaryKey(List<{{metadata.domainClazzName}}> ts) {
    }
    {{/metadata.primaryMetadata}}

    {{#metadata.auditSql}}
    public String auditSql(org.slf4j.Logger log, String sql, Object[] params) {
        return auditSql(log, sql, Arrays.asList(params));
    }

    public String auditSql(org.slf4j.Logger log, String sql, List params) {
        if (sql == null || params == null || params.isEmpty()) {
            return sql;
        }

        String[] parts = sql.split("\\?", params.size() + 1);
        String format = "yyyy-MM-dd HH:mm:ss.SSS";
        java.text.SimpleDateFormat sf = new java.text.SimpleDateFormat(format);
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);

        java.util.StringJoiner joiner = new java.util.StringJoiner("");
        for (int i = 0; i < params.size(); i++) {
            joiner.add(parts[i]);
            Object param = params.get(i);
            if (param instanceof java.util.Date) {
                param = sf.format((java.util.Date) param);
            } else if (param instanceof java.time.temporal.TemporalAccessor) {
                param = formatter.format((java.time.temporal.TemporalAccessor) param);
            } else if (param instanceof byte[]) {
                param = Arrays.toString((byte[]) param);
            } else if (param instanceof Number) {
            } else if (param ==null){
            } else {
                param = String.valueOf(param);
            }
            if(param == null){
                param = "null";
            }else if (param instanceof String) {
                param = "'" + param + "'";
            }
            joiner.add(param.toString());
        }
        joiner.add(parts[parts.length - 1]);

        String auditSql = joiner.toString();
        printAuditSql(log, auditSql);
        return auditSql;
    }

    public void printAuditSql(org.slf4j.Logger log, String sql) {
        log.info("Audit sql:  {}", sql);
    }
    {{/metadata.auditSql}}

    {{^metadata.auditSql}}
    public String auditSql(org.slf4j.Logger log, String sql, List params) {
        return sql;
    }
    public String auditSql(org.slf4j.Logger log, String sql, Object[] params) {
        return sql;
    }
    {{/metadata.auditSql}}

}

