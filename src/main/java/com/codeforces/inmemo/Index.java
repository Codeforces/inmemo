package com.codeforces.inmemo;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class Index<T extends HasId, V> {
    private static final Logger logger = Logger.getLogger(Index.class);

    private static final Object NULL = new Object();

    private final String name;
    private final IndexGetter<T, V> indexGetter;

    // Actually, it has type ConcurrentMap<V, Map<Long, T>> but can't be used because of non-null keys in ConcurrentHashMap.
    private final ConcurrentMap<Object, Map<Long, T>> map = new ConcurrentHashMap<>();

    // Actually, it has type ConcurrentMap<V, Lock> but can't be used because of non-null keys in ConcurrentHashMap.
    private final ConcurrentMap<Object, Lock> locks = new ConcurrentHashMap<>();

    private Table<T> table;

    private final Class<?> indexClass;

    public Index(
            final String name,
            @SuppressWarnings("UnusedParameters") final Class<V> indexClass,
            final IndexGetter<T, V> indexGetter) {
        this.name = name;
        this.indexClass = indexClass;
        this.indexGetter = indexGetter;
    }

    void setTable(final Table<T> table) {
        this.table = table;
    }

    private Object wrapValue(final V value) {
        return value == null ? NULL : value;
    }

    void insertOrUpdate(final T tableItem) {
        final Object value = wrapValue(indexGetter.get(tableItem));

        if (value != NULL && value.getClass() != indexClass) {
            logger.info("Item of " + tableItem.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        if (!map.containsKey(value) || !locks.containsKey(value)) {
            map.putIfAbsent(value, new ConcurrentHashMap<Long, T>());
            locks.putIfAbsent(value, new ReentrantLock());
        }

        final Lock lock = locks.get(value);
        lock.lock();
        try {
            map.get(value).put(tableItem.getId(), tableItem);
        } finally {
            lock.unlock();
        }
    }

    List<T> internalFind(final V value, final Matcher<T> matcher) {
        if (value != null && value.getClass() != indexClass) {
            logger.info("Value of " + value.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        final Object wrappedValue = wrapValue(value);
        final Map<Long, T> valueMap = map.get(wrappedValue);

        if (valueMap == null || valueMap.isEmpty()) {
            return Collections.emptyList();
        } else {
            final List<T> result = new ArrayList<>();

            final Lock lock = locks.get(wrappedValue);
            lock.lock();
            try {
                for (final T tableItem : valueMap.values()) {
                    if (matcher.match(tableItem)) {
                        result.add(tableItem);
                    }
                }
            } finally {
                lock.unlock();
            }

            return result;
        }
    }

    private T internalFindOnly(boolean throwOnNotUnique, V value, Matcher<T> matcher) {
        if (value != null && value.getClass() != indexClass) {
            logger.info("Value of " + value.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        final Object wrappedValue = wrapValue(value);
        final Map<Long, T> valueMap = map.get(wrappedValue);

        if (valueMap == null || valueMap.isEmpty()) {
            return null;
        } else {
            final List<T> result = new ArrayList<>(2);

            final Lock lock = locks.get(wrappedValue);
            lock.lock();
            try {
                for (final T tableItem : valueMap.values()) {
                    if (matcher.match(tableItem)) {
                        result.add(tableItem);
                        if (!throwOnNotUnique) {
                            break;
                        } else {
                            if (result.size() >= 2) {
                                throw new InmemoException("Expected at most one item of " + table.getClazz()
                                        + " matching index " + getName()
                                        + " with value=" + value + ".");
                            }
                        }
                    }
                }
            } finally {
                lock.unlock();
            }

            return result.isEmpty() ? null : result.get(0);
        }
    }

    long internalFindCount(final V value, final Matcher<T> matcher) {
        final Object wrappedValue = wrapValue(value);
        final Map<Long, T> valueMap = map.get(wrappedValue);

        if (valueMap == null || valueMap.isEmpty()) {
            return 0;
        } else {
            //noinspection TooBroadScope
            long result = 0;

            final Lock lock = locks.get(wrappedValue);
            lock.lock();

            try {
                for (final T tableItem : valueMap.values()) {
                    if (matcher.match(tableItem)) {
                        result++;
                    }
                }
            } finally {
                lock.unlock();
            }

            return result;
        }
    }

    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    List<T> find(final Object value, final Matcher<T> predicate) {
        return internalFind((V) value, predicate);
    }

    @SuppressWarnings("unchecked")
    public T findOnly(final boolean throwOnNotUnique, final Object value, final Matcher<T> predicate) {
        return internalFindOnly(throwOnNotUnique, (V) value, predicate);
    }

    @SuppressWarnings("unchecked")
    long findCount(final Object value, final Matcher<T> predicate) {
        return internalFindCount((V) value, predicate);
    }
}
