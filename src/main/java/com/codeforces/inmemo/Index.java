package com.codeforces.inmemo;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings("WeakerAccess")
public class Index<T extends HasId, V> {
    private static final Logger logger = Logger.getLogger(Index.class);

    private static final Object NULL = new Object();

    private final String name;
    private final IndexGetter<T, V> indexGetter;

    // Actually, it has type ConcurrentMap<V, Map<Long, T>> but can't be used because of non-null keys in ConcurrentHashMap.
    private final ConcurrentMap<Object, TLongObjectMap<T>> map;

    // Actually, it has type ConcurrentMap<V, >> but can't be used because of non-null keys in ConcurrentHashMap.
    private final ConcurrentMap<Object, T> uniqueMap;

    private final EmergencyDatabaseHelper<V> emergencyDatabaseHelper;

    // {@code true} iff each index value corresponds to at most one item.
    private final boolean unique;

    private Table<T> table;

    private final Class<?> indexClass;

    private Index(
            String name,
            Class<V> indexClass,
            IndexGetter<T, V> indexGetter,
            boolean unique,
            EmergencyDatabaseHelper<V> emergencyDatabaseHelper) {
        this.name = name;
        this.indexClass = indexClass;
        this.indexGetter = indexGetter;
        this.unique = unique;
        this.emergencyDatabaseHelper = emergencyDatabaseHelper;

        if (unique) {
            uniqueMap = new ConcurrentHashMap<>();
            map = null;
        } else {
            uniqueMap = null;
            map = new ConcurrentHashMap<>();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId, V> Index<T, V> create(String name,
                                                          Class<V> indexClass,
                                                          IndexGetter<T, V> indexGetter) {
        return create(name, indexClass, indexGetter, null);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId, V> Index<T, V> createUnique(String name,
                                                                Class<V> indexClass,
                                                                IndexGetter<T, V> indexGetter) {
        return createUnique(name, indexClass, indexGetter, null);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId, V> Index<T, V> create(String name,
                                                          Class<V> indexClass,
                                                          IndexGetter<T, V> indexGetter,
                                                          EmergencyDatabaseHelper<V> emergencyDatabaseHelper) {
        return new Index<>(name, indexClass, indexGetter, false, emergencyDatabaseHelper);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId, V> Index<T, V> createUnique(String name,
                                                                Class<V> indexClass,
                                                                IndexGetter<T, V> indexGetter,
                                                                EmergencyDatabaseHelper<V> emergencyDatabaseHelper) {
        return new Index<>(name, indexClass, indexGetter, true, emergencyDatabaseHelper);
    }

    void setTable(Table<T> table) {
        this.table = table;
    }

    private Object wrapValue(V value) {
        return value == null ? NULL : value;
    }

    void insertOrUpdate(@Nonnull T tableItem) {
        Object value = wrapValue(indexGetter.get(tableItem));

        if (value != NULL && value.getClass() != indexClass) {
            logger.info("Item of " + tableItem.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        if (unique) {
            assert uniqueMap != null;
            T previousTableItem = uniqueMap.get(value);
            if (previousTableItem != null
                    && previousTableItem.getId() != tableItem.getId()) {
                throw new InmemoException("Index `" + name
                        + "` expected to be unique but it has multiple items for value="
                        + value + " [previousTableItem=" + previousTableItem + ", newTableItem=" + tableItem + "].");
            }

            uniqueMap.put(value, tableItem);
        } else {
            assert map != null;
            if (!map.containsKey(value)) {
                //map.putIfAbsent(value, new ConcurrentHashMap<>());
                map.putIfAbsent(value, new TLongObjectHashMap<>(1));
            }

            map.get(value).put(tableItem.getId(), tableItem);
        }
    }

    private List<T> internalFind(V value, Matcher<T> matcher) {
        if (unique) {
            T tableItem = internalFindOnly(true, value, matcher);
            if (tableItem == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(tableItem);
            }
        }

        if (value != null && value.getClass() != indexClass) {
            logger.info("Value of " + value.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        Object wrappedValue = wrapValue(value);

        assert map != null;
        TLongObjectMap<T> valueMap = map.get(wrappedValue);

        if ((valueMap == null || valueMap.isEmpty()) && emergencyDatabaseHelper == null) {
            return Collections.emptyList();
        }

        Collection<T> tableItems = (valueMap == null || valueMap.isEmpty())
                ? table.findAndUpdateByEmergencyQueryFields(emergencyDatabaseHelper.getEmergencyQueryFields(value))
                : values(valueMap);

        List<T> result = new ArrayList<>(tableItems.size());

        for (T tableItem : tableItems) {
            if (matcher.match(tableItem)) {
                result.add(tableItem);
            }
        }

        return result;
    }

    private Collection<T> values(TLongObjectMap<T> map) {
        List<T> result = new ArrayList<>(map.size());
        for (TLongObjectIterator<T> i = map.iterator(); i.hasNext(); ) {
            i.advance();
            result.add(i.value());
        }
        return result;
    }

    private T internalFindOnly(boolean throwOnNotUnique, V value, Matcher<T> matcher) {
        if (value != null && value.getClass() != indexClass) {
            logger.info("Value of " + value.getClass() + " is invalid for index '"
                    + table.getClazz().getName() + '#' + name + "'.");
        }

        Object wrappedValue = wrapValue(value);

        if (unique) {
            assert uniqueMap != null;
            T tableItem = uniqueMap.get(wrappedValue);

            if (tableItem == null && emergencyDatabaseHelper != null) {
                List<T> items = table.findAndUpdateByEmergencyQueryFields(
                        emergencyDatabaseHelper.getEmergencyQueryFields(value)
                );

                if (!items.isEmpty()) {
                    tableItem = items.get(0);
                }

                if (throwOnNotUnique && items.size() >= 2) {
                    throw new InmemoException("Expected at most one item of " + table.getClazz()
                            + " matching index " + name
                            + " with value=" + value + '.');
                }
            }

            if (tableItem == null || !matcher.match(tableItem)) {
                return null;
            } else {
                return tableItem;
            }
        } else {
            assert map != null;
            TLongObjectMap<T> valueMap = map.get(wrappedValue);

            if ((valueMap == null || valueMap.isEmpty()) && emergencyDatabaseHelper == null) {
                return null;
            }

            Collection<T> tableItems = (valueMap == null || valueMap.isEmpty())
                    ? table.findAndUpdateByEmergencyQueryFields(emergencyDatabaseHelper.getEmergencyQueryFields(value))
                    : values(valueMap);

            List<T> result = new ArrayList<>(2);

            for (T tableItem : tableItems) {
                if (matcher.match(tableItem)) {
                    result.add(tableItem);
                    if (throwOnNotUnique) {
                        if (result.size() >= 2) {
                            throw new InmemoException("Expected at most one item of " + table.getClazz()
                                    + " matching index " + name
                                    + " with value=" + value + '.');
                        }
                    } else {
                        break;
                    }
                }
            }

            return result.isEmpty() ? null : result.get(0);
        }
    }

    long internalFindCount(V value, Matcher<T> matcher) {
        return internalFind(value, matcher).size();
    }

    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    List<T> find(Object value, Matcher<T> predicate) {
        return internalFind((V) value, predicate);
    }

    @SuppressWarnings("unchecked")
    public T findOnly(boolean throwOnNotUnique, Object value, Matcher<T> predicate) {
        return internalFindOnly(throwOnNotUnique, (V) value, predicate);
    }

    @SuppressWarnings("unchecked")
    long findCount(Object value, Matcher<T> predicate) {
        return internalFindCount((V) value, predicate);
    }

    /**
     * Helper interface to emergency query database if object is not found in memory.
     */
    public interface EmergencyDatabaseHelper<V> {
        /**
         * @param indexValue index value
         * @return mixed array of pairs: (database column name, value). So the length of array is always even.
         */
        Object[] getEmergencyQueryFields(@Nullable V indexValue);
    }
}
