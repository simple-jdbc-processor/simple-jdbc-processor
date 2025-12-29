package io.github.simple.jdbc.processor.util;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.simple.jdbc.processor.util.CamelUtils.toSnake;

@SuppressWarnings("unchecked")
public class EntityHelper {

    private static final Map<Class<?>, SetGetInvoker<?>> CLASS_INVOKER = new ConcurrentHashMap<>();

    private static final Map<String, List<String>> COLUMN_MAP = new ConcurrentHashMap<>();

    private EntityHelper() {

    }

    public static <T> List<T> getResultList(String cacheColumnKey, ResultSet resultSet, Class<T> targetClass) throws SQLException {
        List<String> columnList = COLUMN_MAP.get(cacheColumnKey);
        if (columnList == null) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnLabel = metaData.getColumnLabel(i);
                columnList = COLUMN_MAP.computeIfAbsent(cacheColumnKey, k -> new ArrayList<>());
                if (columnLabel != null) {
                    columnList.add(columnLabel);
                } else if (columnName != null) {
                    columnList.add(columnName);
                }
            }
        }
        if (columnList == null) {
            return new ArrayList<>();
        }
        List<T> result = new ArrayList<>();
        while (resultSet.next()) {
            T t = newInstance(targetClass);
            result.add(t);
            for (int i = 0; i < columnList.size(); i++) {
                String columnName = columnList.get(i);
                int index = i + 1;
                Class<?> columnClazz = getColumnClazz(targetClass, columnName);
                Object value = null;
                if (columnClazz.isEnum()) {
                    String str = resultSet.getString(index);
                    if (str != null) {
                        value = Enum.valueOf((Class<Enum>) columnClazz, str);
                    }
                } else {
                    value = resultSet.getObject(index, columnClazz);
                }
                if (value != null) {
                    set(targetClass, columnName, t, value);
                }
            }
        }
        return result;
    }

    public static <T> T newInstance(Class<T> clazz) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return (T) setGetInvoker.newInstance();
    }

    public static Object get(Class<?> clazz, String name, Object instance) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.get(name, instance);
    }

    public static void set(Class<?> clazz, String name, Object instance, Object param) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        setGetInvoker.set(name, instance, param);
    }

    public static Class<?> getColumnClazz(Class<?> clazz, String name) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.getColumnClazz(name);
    }

    public static List<String> getColumns(Class<?> clazz) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.getColumns(clazz);
    }

    public static String getColumnsStr(Class<?> clazz) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.getColumnsStr(clazz);
    }

    public static String getTable(Class<?> clazz) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.getTable(clazz);
    }

    public static void setPrimary(Class<?> clazz, Object instance, long param) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        setGetInvoker.setPrimaryKey(clazz, instance, param);
    }

    public static List<Object> getValues(Class<?> clazz, Object instance) {
        SetGetInvoker<?> setGetInvoker = CLASS_INVOKER.computeIfAbsent(clazz, k -> new SetGetInvoker<>(clazz));
        return setGetInvoker.getValues(clazz, instance);
    }


    public static class SetGetInvoker<T> {

        private final Map<String, MethodHandle> getMethodHandleMap = new LinkedHashMap<>();

        private final Map<String, MethodHandle> setMethodHandleMap = new LinkedHashMap<>();

        private final Map<String, Class<?>> columnClazzMap = new LinkedHashMap<>();

        private final Map<Class<?>, String> tableMap = new HashMap<>();

        private final Map<Class<?>, List<String>> columnsMap = new HashMap<>();

        private final Map<Class<?>, String> columnsStrMap = new HashMap<>();

        private final Map<Class<?>, MethodHandle> primaryKeyMap = new HashMap<>();

        private MethodHandle constructor;

        public SetGetInvoker(Class<?> clazz) {
            lookup(clazz);
        }

        private synchronized void lookup(Class<?> clazz) {
            if (this.constructor != null) {
                return;
            }
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                for (Method method : clazz.getMethods()) {
                    String methodName = method.getName();
                    String name = methodToProperty(methodName);
                    if (methodName.startsWith("get") || methodName.startsWith("is")) {
                        MethodType methodType = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                        MethodHandle methodHandle = lookup.findVirtual(clazz, methodName, methodType);
                        getMethodHandleMap.put(name, methodHandle);
                        getMethodHandleMap.put(toSnake(name), methodHandle);
                        columnClazzMap.put(name, method.getReturnType());
                        columnClazzMap.put(toSnake(name), method.getReturnType());
                    }
                    if (methodName.startsWith("set")) {
                        MethodType methodType = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                        MethodHandle methodHandle = lookup.findVirtual(clazz, methodName, methodType);
                        setMethodHandleMap.put(name, methodHandle);
                        setMethodHandleMap.put(toSnake(name), methodHandle);
                    }
                }

                Table table = clazz.getAnnotation(Table.class);
                if (table != null && !"".equalsIgnoreCase(table.name())) {
                    tableMap.put(clazz, table.name());
                } else {
                    tableMap.put(clazz, toSnake(clazz.getSimpleName()));
                }

                List<String> columns = new ArrayList<>();
                for (Field field : clazz.getDeclaredFields()) {
                    String name = field.getName();
                    if (field.getAnnotation(Transient.class) != null || Modifier.isTransient(field.getModifiers())) {
                        continue;
                    }

                    MethodHandle getMethodHandle = getMethodHandleMap.get(name);
                    if (getMethodHandle == null) {
                        continue;
                    }

                    MethodHandle setMethodHandle = setMethodHandleMap.get(name);
                    if (setMethodHandle == null) {
                        continue;
                    }

                    String snake = toSnake(name);
                    getMethodHandleMap.put(snake, getMethodHandle);
                    setMethodHandleMap.put(snake, setMethodHandle);

                    Column column = field.getAnnotation(Column.class);
                    if (column != null && !"".equals(column.name())) {
                        String columnName = column.name()
                                .replace("\"", "")
                                .replace("`", "")
                                .replace("[", "")
                                .replace("]", "");
                        getMethodHandleMap.put(columnName, getMethodHandle);
                        setMethodHandleMap.put(columnName, setMethodHandle);
                        columns.add(column.name());
                    } else {
                        columns.add(toSnake(field.getName()));
                    }

                    Id primaryKey = field.getAnnotation(Id.class);
                    if (primaryKey != null) {
                        primaryKeyMap.put(clazz, setMethodHandle);
                    }
                    columnClazzMap.put(name, field.getType());
                    columnClazzMap.put(snake, field.getType());

                }
                columnsMap.put(clazz, columns);
                columnsStrMap.put(clazz, String.join(", ", columns));
                MethodType noArgType = MethodType.methodType(void.class); // 无参数
                this.constructor = lookup.findConstructor(clazz, noArgType);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public void set(String name, Object target, Object arg) {
            try {
                MethodHandle methodHandle = setMethodHandleMap.get(name);
                methodHandle.invoke(target, arg);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public Object get(String name, Object target) {
            try {
                MethodHandle methodHandle = getMethodHandleMap.get(name);
                return methodHandle.invoke(target);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public T newInstance() {
            try {
                return (T) constructor.invoke();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public Class<?> getColumnClazz(String name) {
            return columnClazzMap.get(name);
        }

        public List<String> getColumns(Class<?> clazz) {
            return columnsMap.get(clazz);
        }

        public String getColumnsStr(Class<?> clazz) {
            return columnsStrMap.get(clazz);
        }

        public String getTable(Class<?> clazz) {
            return tableMap.get(clazz);
        }

        public String getPrimaryKey(Class<?> clazz) {
            return null;
        }

        public void setPrimaryKey(Class<?> clazz, Object instance, long param) {
            try {
                MethodHandle methodHandle = primaryKeyMap.get(clazz);
                Class<?> parameterType = methodHandle.type().parameterType(1);
                if (Integer.class.equals(parameterType) || int.class.equals(parameterType)) {
                    methodHandle.invoke(instance, (int) param);
                } else if (Short.class.equals(parameterType) || short.class.equals(parameterType)) {
                    methodHandle.invoke(instance, (short) param);
                } else {
                    methodHandle.invoke(instance, param);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public List<Object> getValues(Class<?> clazz, Object instance) {
            List<String> columns = getColumns(clazz);
            List<Object> values = new ArrayList<>(columns.size());
            for (String column : columns) {
                Object value = get(column, instance);
                values.add(value);
            }
            return values;
        }

        public Class<?> getColumnType(String column) {
            return columnClazzMap.get(column);
        }
    }


    private static String methodToProperty(String methodName) {
        if (methodName == null || methodName.isEmpty()) {
            return "";
        }

        String prefix;
        if (methodName.startsWith("is")) {
            prefix = "is";
        } else if (methodName.startsWith("get")) {
            prefix = "get";
        } else if (methodName.startsWith("set")) {
            prefix = "set";
        } else {
            return null;
        }

        String name = methodName.substring(prefix.length());
        if (name.isEmpty()) {
            return name;
        }

        if (name.length() == 1 || !Character.isUpperCase(name.charAt(1))) {
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
        }
        return name;
    }

}
