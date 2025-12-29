package io.github.simple.jdbc.processor;

import java.io.Serializable;
import java.util.List;

public class PageInfo<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private int page;

    private int size;

    private long total;

    private int pages;

    private List<T> data;

    private PageInfo(){};

    public PageInfo(int page, int size, long total, List<T> data) {
        this.page = page;
        this.size = size;
        this.total = total;
        this.data = data;
        if (total == 0) {
            this.pages = 0;
        } else {
            this.pages = (int) ((total / size) + ((total % size) > 0 ? 1 : 0));
        }
    }

    public int getPage() {
        return page;
    }

    public PageInfo<T> setPage(int page) {
        this.page = page;
        return this;
    }

    public int getSize() {
        return size;
    }

    public PageInfo<T> setSize(int size) {
        this.size = size;
        return this;
    }

    public long getTotal() {
        return total;
    }

    public PageInfo<T> setTotal(long total) {
        this.total = total;
        return this;
    }

    public int getPages() {
        return pages;
    }

    public PageInfo<T> setPages(int pages) {
        this.pages = pages;
        return this;
    }

    public List<T> getData() {
        return data;
    }

    public PageInfo<T> setData(List<T> data) {
        this.data = data;
        return this;
    }

    public boolean hasPrevious() {
        return this.page > 1L;
    }

    public boolean hasNext() {
        return this.page < this.getPages();
    }

    @Override
    public String toString() {
        return "PageInfo{" +
                "page=" + page +
                ", size=" + size +
                ", total=" + total +
                ", pages=" + pages +
                ", data=" + data +
                '}';
    }
}
