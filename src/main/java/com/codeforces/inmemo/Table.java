package com.codeforces.inmemo;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.jacuzzi.core.Row;
import org.springframework.beans.BeanUtils;

import javax.annotation.Nonnull;
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
    private final List<RowListener> rowListeners = new ArrayList<>();
    private final List<ItemListener<T>> itemListeners = new ArrayList<>();

    private final Class<T> clazz;
    private final String clazzSpec;
    private final String indicatorField;
    private final String databaseIndex;
    private TableUpdater<T> tableUpdater;
    private volatile boolean preloaded;
    private final TLongSet ids = new TLongHashSet();

    Table(final Class<T> clazz, final String indicatorField) {
        this.clazz = clazz;
        if (indicatorField.contains("@")) {
            String[] tokens = indicatorField.split("@");
            this.indicatorField = tokens[0];
            this.databaseIndex = tokens[1];
        } else {
            this.indicatorField = indicatorField;
            this.databaseIndex = null;
        }
        clazzSpec = ReflectionUtil.getTableClassSpec(clazz);
    }

    public void createUpdater(Object initialIndicatorValue) {
        tableUpdater = new TableUpdater<>(this, initialIndicatorValue);
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

    String getDatabaseIndex() {
        return databaseIndex;
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
                return new Matcher<T>() {
                    @Override
                    public boolean match(final T tableItem) {
                        final U otherItem = ReflectionUtil.newInstance(otherClass);
                        BeanUtils.copyProperties(tableItem, otherItem);
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

    void add(final RowListener rowListener) {
        rowListeners.add(rowListener);
    }

    void add(final ItemListener<T> itemListener) {
        itemListeners.add(itemListener);
    }

    boolean hasInsertOrUpdateByRow() {
        return !rowListeners.isEmpty();
    }

    @SuppressWarnings("unchecked")
    <U extends HasId> void insertOrUpdate(@Nonnull final U item) {
        final Class<?> itemClass = item.getClass();
        final String itemClassSpec = ReflectionUtil.getTableClassSpec(itemClass);

        if (clazzSpec.equals(itemClassSpec)) {
            final T tableItem = ReflectionUtil.newInstance(clazz);
            BeanUtils.copyProperties(item, tableItem);
            internalInsertOrUpdate(tableItem);
        } else {
            throw new InmemoException("Table class is incompatible with the class of object [tableClass=" + clazz
                    + ", clazz=" + itemClass + "].");
        }
    }

    void insertOrUpdate(final Row row) {
        for (RowListener rowListener : rowListeners) {
            rowListener.insertOrUpdate(row);
        }
    }

    int size() {
        lock.lock();
        try {
            return ids.size();
        } finally {
            lock.unlock();
        }
    }

    private void internalInsertOrUpdate(@Nonnull final T item) {
        lock.lock();
        try {
            ids.add(item.getId());
            for (final Index<T, ?> index : indices.values()) {
                index.insertOrUpdate(item);
            }
            for (final ItemListener<T> itemListener : itemListeners) {
                itemListener.insertOrUpdate(item);
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

    public T findOnly(final boolean throwOnNotUnique, final IndexConstraint<?> indexConstraint, final Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        final Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.findOnly(throwOnNotUnique, indexConstraint.getValue(), predicate);
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

    public void insertOrUpdateByIds(Long[] ids) {
        for (Long id : ids) {
            if (id == null) {
                continue;
            }

            tableUpdater.insertOrUpdateById(id);
        }
    }
}
