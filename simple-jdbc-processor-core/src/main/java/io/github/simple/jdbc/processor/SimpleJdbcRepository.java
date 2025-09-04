package io.github.simple.jdbc.processor;


import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface SimpleJdbcRepository<T, ID, Example> {

    T selectByPrimaryKey(ID id);

    T selectByPrimaryKeyForUpdate(ID id);

    List<T> selectByPrimaryKeys(List<ID> ids);

    List<T> selectByExample(Example query);

    void consumeByExample(Example query, Consumer<T> consumer);

    T selectOne(Example query);

    List<T> selectAll();

    void insert(T t);

    void insertSelective(T t);

    void upsertSelective(T t);

    void insertBatch(List<T> ts);

    long countByExample(Example query);

    int updateByPrimaryKeySelective(T t);

    int updateByPrimaryKey(T t);

    int updateByExampleSelective(T t, Example query);

    int updateByExample(T t, Example query);

    int deleteByPrimaryKey(ID id);

    int deleteByPrimaryKeys(List<ID> ids);

    int deleteByExample(Example query);

    boolean existsById(ID id);

    boolean existsByExample(Example query);

    List<T> selectByPrimaryKeysWithSorted(List<ID> ids);

    Map<ID, T> mapById(List<ID> ids);
}