package com.codeforces.inmemo;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class IndexConstraint<V> {
    private final String indexName;
    private final V value;

    public IndexConstraint(String indexName, V value) {
        this.indexName = indexName;
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public String getIndexName() {
        return indexName;
    }
}
