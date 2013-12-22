package com.codeforces.inmemo;

import net.sf.cglib.beans.BeanCopier;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>
 * Inmemo is a framework to store entities in-memory and update their state in memory automatically on each update
 * in database. Each entity should have `indicatorField` which updates to greater value on each change. The best choice
 * is `updateTime` field of MySQL type `TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`. It
 * supports indices (based on hashmaps) and queries to find list of entities or counts. Refer to examples for
 * clarifications.
 * </p>
 * <p>
 * The implementation is friendly for class-reloading magic like in <a href="http://code.google.com/p/nocturne">Nocturne</a>
 * framework.
 * </p>
 */
public final class Inmemo {
    private static final Logger logger = Logger.getLogger(Inmemo.class);
    private static final Map<String, Table<? extends HasId>> tables = new ConcurrentHashMap<>();
    private static final Lock tablesLock = new ReentrantLock();

    private Inmemo() {
        // No operations.
    }

    @SuppressWarnings("UnusedDeclaration")
    public static boolean isPreloaded() {
        tablesLock.lock();
        try {
            for (final Table<? extends HasId> table : tables.values()) {
                if (!table.isPreloaded()) {
                    return false;
                }
            }
        } finally {
            tablesLock.unlock();
        }

        return true;
    }

    /**
     * Creates new table, if there is already table for compatible class then doing nothing.
     *
     * @param clazz          Table item class.
     * @param indicatorField Item field which will be monitored to increase on each change. Good idea to make it
     *                       'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'.
     * @param indices        Indices built with Indices.Builder.
     * @param <T>            Item class.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> void createTable(
            @Nonnull final Class<T> clazz,
            @Nonnull final String indicatorField,
            @Nonnull final Indices<T> indices) {
        tablesLock.lock();

        try {
            final String tableClassName = ReflectionUtil.getTableClassName(clazz);

            final Table<? extends HasId> table = tables.get(tableClassName);
            if (table == null) {
                renewTable(clazz, indicatorField, indices);
            } else {
                // Exactly the same class?
                if (table.getClazz().equals(clazz)) {
                    logger.info("Exactly the same class [class=" + clazz + "].");
                    return;
                }

                final String clazzSpec = ReflectionUtil.getTableClassSpec(clazz);
                // Compatible classes?
                if (table.getClazzSpec().equals(clazzSpec)) {
                    logger.info("Compatible classes " + tableClassName + " [class=" + clazz + "].");
                    return;
                }

                renewTable(clazz, indicatorField, indices);
            }
        } finally {
            tablesLock.unlock();
        }
    }

    /**
     * Drops table or doing nothing.
     *
     * @param clazz Table class.
     * @param <T>   Entity class.
     */
    public static <T> void dropTableIfExists(@Nonnull final Class<T> clazz) {
        final String tableClassName = ReflectionUtil.getTableClassName(clazz);
        final Table<? extends HasId> table = tables.get(tableClassName);

        if (table != null) {
            tablesLock.lock();
            try {
                tables.remove(tableClassName);
            } finally {
                tablesLock.unlock();
            }
        }
    }

    private static <T extends HasId> void renewTable(final Class<T> clazz, final String indicatorField, final Indices<T> indices) {
        final Table<T> table = new Table<>(clazz, indicatorField);
        table.createUpdater();

        for (final Index<T, ?> index : indices.getIndices()) {
            table.add(index);
        }

        table.runUpdater();
        tables.put(ReflectionUtil.getTableClassName(clazz), table);
    }

    /**
     * @param clazz           Table item class.
     * @param indexConstraint Index to use in search, index value.
     * @param matcher         Predicate to choose items.
     * @param <T>             Items class.
     * @return List of _copies_ of matched items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> List<T> find(
            @Nonnull final Class<T> clazz,
            @Nonnull final IndexConstraint<?> indexConstraint,
            @Nonnull final Matcher<T> matcher) {
        final String tableClassName = ReflectionUtil.getTableClassName(clazz);

        final Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        if (!table.isCompatibleItemClass(clazz)) {
            throw new InmemoException("Table class is incompatible with the given [tableClass=" + table.getClazz()
                    + ", clazz=" + clazz + "].");
        }

        //noinspection rawtypes
        final Matcher tableMatcher = table.convertMatcher(clazz, matcher);

        //noinspection unchecked
        final List<? extends HasId> result = table.find(indexConstraint, tableMatcher);
        final BeanCopier beanCopier = BeanCopier.create(table.getClazz(), clazz, false);
        final List<T> tableClassResult = new ArrayList<>(result.size());

        for (final HasId tableItem : result) {
            final T item = ReflectionUtil.newInstance(clazz);
            beanCopier.copy(tableItem, item, null);
            tableClassResult.add(item);
        }

        return tableClassResult;
    }

    /**
     * @param clazz           Table item class.
     * @param indexConstraint Index to use in search, index value.
     * @param matcher         Predicate to choose items.
     * @param <T>             Items class.
     * @return Number of matched items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> long findCount(
            @Nonnull final Class<T> clazz,
            @Nonnull final IndexConstraint<?> indexConstraint,
            @Nonnull final Matcher<T> matcher) {
        final String tableClassName = ReflectionUtil.getTableClassName(clazz);

        final Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        if (!table.isCompatibleItemClass(clazz)) {
            throw new InmemoException("Table class is incompatible with the given [tableClass=" + table.getClazz()
                    + ", clazz=" + clazz + "].");
        }

        //noinspection rawtypes
        final Matcher tableMatcher = table.convertMatcher(clazz, matcher);

        //noinspection unchecked
        return table.findCount(indexConstraint, tableMatcher);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> void insertOrUpdate(@Nonnull final T object) {
        final String tableClassName = ReflectionUtil.getTableClassName(object.getClass());

        final Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        table.insertOrUpdate(object);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void setDataSource(@Nonnull final DataSource dataSource) {
        TableUpdater.setDataSource(dataSource);
    }
}
