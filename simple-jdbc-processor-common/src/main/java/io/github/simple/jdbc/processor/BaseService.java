package io.github.simple.jdbc.processor;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class BaseService<T, ID, Example> {

    private SimpleJdbcRepository<T, ID, Example> repository;

    public BaseService() {
    }

    public BaseService(SimpleJdbcRepository<T, ID, Example> repository) {
        this.repository = repository;
    }

    public T selectByPrimaryKey(ID id) {
        return repository.selectByPrimaryKey(id);
    }

    public List<T> selectByPrimaryKeys(List<ID> ids) {
        return repository.selectByPrimaryKeys(ids);
    }

    public List<T> selectByPrimaryKeysWithSorted(List<ID> ids) {
        return repository.selectByPrimaryKeysWithSorted(ids);
    }

    public List<T> selectByExample(Example example) {
        return this.repository.selectByExample(example);
    }

    public void consumeByExample(Example example, Consumer<T> consumer) {
        this.repository.consumeByExample(example, consumer);
    }

    public T selectOne(Example example) {
        return this.repository.selectOne(example);
    }

    public void insert(T t) {
        this.repository.insert(t);
    }

    public void insertSelective(T t) {
        this.repository.insertSelective(t);
    }

    public void insertBatch(List<T> ts) {
        this.repository.insertBatch(ts);
    }

    public void insertIgnoreBatch(List<T> ts) {
        this.repository.insertIgnoreBatch(ts);
    }

    public void upsertSelective(T t) {
        this.repository.upsertSelective(t);
    }

    public int updateByPrimaryKeySelective(T t) {
        return this.repository.updateByPrimaryKeySelective(t);
    }

    public int updateByExampleSelective(T t, Example q) {
        return this.repository.updateByExampleSelective(t, q);
    }

    public int updateByExample(T t, Example q) {
        return this.repository.updateByExample(t, q);
    }

    public int updateByPrimaryKey(T t) {
        return this.repository.updateByPrimaryKey(t);
    }

    public long countByExample(Example example) {
        return this.repository.countByExample(example);
    }

    public int deleteByPrimaryKey(ID id) {
        return this.repository.deleteByPrimaryKey(id);
    }

    public int deleteByPrimaryKeys(List<ID> ids) {
        return this.repository.deleteByPrimaryKeys(ids);
    }

    public int deleteByExample(Example q) {
        return this.repository.deleteByExample(q);
    }

    public Map<ID, T> mapById(List<ID> ids) {
        return repository.mapById(ids);
    }

    public PageInfo<T> pageByExample(int page, int size, Example q) {
        List<T> ts = repository.selectByExample(q);
        long total = ts.size();
        if (page == 1 && total < size) {
            total = ts.size();
        } else {
            total = repository.countByExample(q);
        }
        return new PageInfo<>(page, size, total, ts);
    }

    @Autowired
    protected synchronized void setRepository(SimpleJdbcRepository<T, ID, Example> repository) {
        if (this.repository == null) {
            this.repository = repository;
        }
    }
}
