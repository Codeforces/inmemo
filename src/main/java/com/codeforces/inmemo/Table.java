package com.codeforces.inmemo;

import net.sf.cglib.beans.BeanCopier;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class Table<T extends HasId> {
    private final Lock lock = new ReentrantLock();
    private final Map<String, Index<T, ?>> indices = new ConcurrentHashMap<>();
    private final Class<T> clazz;
    private final String clazzSpec;
    private final String indicatorField;
    private TableUpdater<T> tableUpdater;
    private volatile boolean preloaded;

    Table(final Class<T> clazz, final String indicatorField) {
        this.clazz = clazz;
        this.indicatorField = indicatorField;
        clazzSpec = ReflectionUtil.getTableClassSpec(clazz);
    }

    public void createUpdater() {
        tableUpdater = new TableUpdater<>(this);
    }

    void runUpdater() {
        tableUpdater.start();
    }

    Class<T> getClazz() {
        return clazz;
    }

    String getIndicatorField() {
        return indicatorField;
    }

    String getClazzSpec() {
        return clazzSpec;
    }

    boolean isPreloaded() {
        return preloaded;
    }

    void setPreloaded(final boolean preloaded) {
        this.preloaded = preloaded;
    }

    boolean isCompatibleItemClass(final Class<? extends HasId> otherItemClass) {
        return clazz == otherItemClass
                || clazzSpec.equals(ReflectionUtil.getTableClassSpec(otherItemClass));
    }

    @SuppressWarnings("unchecked")
    <U extends HasId> Matcher<T> convertMatcher(final Class<U> otherClass, final Matcher<U> otherMatcher) {
        if (clazz == otherClass) {
            return (Matcher<T>) otherMatcher;
        } else {
            final String otherClassSpec = ReflectionUtil.getTableClassSpec(otherClass);
            if (clazzSpec.equals(otherClassSpec)) {
                final BeanCopier beanCopier = BeanCopier.create(clazz, otherClass, false);

                return new Matcher<T>() {
                    @Override
                    public boolean match(final T tableItem) {
                        final U otherItem = ReflectionUtil.newInstance(otherClass);
                        beanCopier.copy(tableItem, otherItem, null);
                        return otherMatcher.match(otherItem);
                    }
                };
            }
        }

        throw new InmemoException("Can't convert matchers because the are incompatible [class=" + clazz
                + ", otherClass=" + otherClass + "].");
    }

    <V> void add(final Index<T, V> index) {
        indices.put(index.getName(), index);
        index.setTable(this);
    }

    @SuppressWarnings("unchecked")
    <U extends HasId> void insertOrUpdate(final U item) {
        final Class<?> itemClass = item.getClass();
        final String itemClassSpec = ReflectionUtil.getTableClassSpec(itemClass);

        if (clazzSpec.equals(itemClassSpec)) {
            final BeanCopier beanCopier = BeanCopier.create(itemClass, clazz, false);
            final T tableItem = ReflectionUtil.newInstance(clazz);
            beanCopier.copy(item, tableItem, null);
            internalInsertOrUpdate(tableItem);
        } else {
            throw new InmemoException("Table class is incompatible with the class of object [tableClass=" + clazz
                    + ", clazz=" + itemClass + "].");
        }
    }

    private void internalInsertOrUpdate(final T item) {
        lock.lock();
        try {
            for (final Index<T, ?> index : indices.values()) {
                index.insertOrUpdate(item);
            }
        } finally {
            lock.unlock();
        }
    }

    List<T> find(final IndexConstraint<?> indexConstraint, final Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        final Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.find(indexConstraint.getValue(), predicate);
    }

    long findCount(final IndexConstraint<?> indexConstraint, final Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        final Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.findCount(indexConstraint.getValue(), predicate);
    }
}
