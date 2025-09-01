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
            ps.setNull(paramIndex, JDBCType.NULL.getVendorTypeNumber());
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

        if (param instanceof byte[]) {
            ps.setBytes(paramIndex, (byte[]) param);
            return;
        }

        if (param instanceof Enum) {
            ps.setString(paramIndex, ((Enum<?>) param).name());
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

        if (param instanceof java.time.LocalDate) {
            ps.setDate(paramIndex, java.sql.Date.valueOf((java.time.LocalDate) param));
            return;
        } else if (param instanceof java.time.LocalTime) {
            ps.setTime(paramIndex, java.sql.Time.valueOf((java.time.LocalTime) param));
            return;
        } else if (param instanceof java.time.LocalDateTime) {
            ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf((java.time.LocalDateTime) param));
            return;
        } else if (param instanceof java.time.ZonedDateTime) {
            ps.setTimestamp(paramIndex, java.sql.Timestamp.from(((java.time.ZonedDateTime) param).toInstant()));
            return;
        } else if (param instanceof java.util.Calendar){
            ps.setTimestamp(paramIndex, java.sql.Timestamp.from(((java.util.Calendar) param).toInstant()));
        }

        if (param instanceof UUID) {
            ps.setString(paramIndex, ((UUID) param).toString());
            return;
        }

        if (param instanceof Number) {
            if (param instanceof Byte) {
                ps.setByte(paramIndex, (Byte) param);
            } else if (param instanceof Short) {
                ps.setShort(paramIndex, (Short) param);
            } else if (param instanceof Integer) {
                ps.setInt(paramIndex, (Integer) param);
            } else if (param instanceof Long) {
                ps.setLong(paramIndex, (Long) param);
            } else if (param instanceof Float) {
                ps.setFloat(paramIndex, (Float) param);
            } else if (param instanceof Double) {
                ps.setDouble(paramIndex, (Double) param);
            } else if (param instanceof BigDecimal) {
                ps.setBigDecimal(paramIndex, (BigDecimal) param);
            } else if (param instanceof BigInteger) {
                ps.setBigDecimal(paramIndex, new BigDecimal((BigInteger) param));
            }
            return;
        }

        ps.setObject(paramIndex, param);
    }


    public {{metadata.domainClazzName}} handle(ResultSet rs, List<String> columns) throws SQLException {
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        for (String column : columns) {
            {{#metadata.columnMetadataList}}
            if ("{{originColumnName}}".equals(column) || "{{columnName}}".equals(column) || "{{fieldName}}".equals(column)) {
                decode{{firstUpFieldName}}(rs, t, "{{originColumnName}}", {{javaType}}.class);
            }
            {{/metadata.columnMetadataList}}
        }
        return t;
    }


    public {{metadata.domainClazzName}} handle(ResultSet rs) throws SQLException {
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        {{#metadata.columnMetadataList}}
        decode{{firstUpFieldName}}(rs, t, "{{originColumnName}}", {{javaType}}.class);
        {{/metadata.columnMetadataList}}
        return t;
    }

    {{#metadata.columnMetadataList}}
    public void decode{{firstUpFieldName}}(ResultSet resultSet, {{metadata.domainClazzName}} t, String name, Class<{{javaType}}> targetType) throws SQLException {
        {{#isEnums}}
        String value = resultSet.getString(name);
        if(value == null){
            return;
        }
        t.set{{firstUpFieldName}}(Enum.valueOf(targetType, value));
        {{/isEnums}}
        {{^isEnums}}
        {{javaType}} value = resultSet.getObject(name, targetType);
        if(value == null){
            return;
        }
        t.set{{firstUpFieldName}}(value);
        {{/isEnums}}
    }

    public Object encode{{firstUpFieldName}}({{javaType}} value) {
        {{^isEnums}}return value;{{/isEnums}}{{#isEnums}}return value == null ? null: String.valueOf(value);{{/isEnums}}
    }

    public List encode{{firstUpFieldName}}List(List<{{javaType}}> values) {
        {{^isEnums}}return values;{{/isEnums}}
        {{#isEnums}}return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.toList());{{/isEnums}}
    }

    {{/metadata.columnMetadataList}}
}

