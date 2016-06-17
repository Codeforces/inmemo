package com.codeforces.inmemo;

import net.sf.cglib.beans.BeanCopier;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
    private static final ConcurrentMap<ClassPair, BeanCopier> beanCopiers = new ConcurrentHashMap<>();

    private Inmemo() {
        // No operations.
    }

    private static BeanCopier getBeanCopier(Class<?> sourceClass, Class<?> targetClass) {
        ClassPair classPair = new ClassPair(sourceClass, targetClass);
        BeanCopier beanCopier = beanCopiers.get(classPair);
        if (beanCopier == null) {
            BeanCopier copier = BeanCopier.create(sourceClass, targetClass, false);
            logger.info("Created BeanCopier(" + sourceClass + ',' + targetClass + ").");
            beanCopiers.putIfAbsent(classPair, copier);
            return beanCopiers.get(classPair);
        } else {
            return beanCopier;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> Matcher<T> acceptAnyMatcher() {
        return new Matcher<T>() {
            @Override
            public boolean match(T tableItem) {
                return true;
            }
        };
    }

    @SuppressWarnings("UnusedDeclaration")
    public static boolean isPreloaded() {
        tablesLock.lock();
        try {
            for (Table<? extends HasId> table : tables.values()) {
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
     * @param clazz                 Table item class.
     * @param indicatorField        Item field which will be monitored to increase on each change. Good idea to make it
     *                              'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'.
     * @param initialIndicatorValue Initial value of indicator, will be loaded only items with at least
     *                              {@code initialIndicatorValue}. Use {@code null} to load all the items.
     * @param indices               Indices built with Indices.Builder.
     * @param <T>                   Item class.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> void createTable(
            @Nonnull Class<T> clazz,
            @Nonnull String indicatorField,
            @Nullable Object initialIndicatorValue,
            @Nonnull Indices<T> indices,
            boolean waitForPreload) {
        tablesLock.lock();

        try {
            String tableClassName = ReflectionUtil.getTableClassName(clazz);

            Table<? extends HasId> table = tables.get(tableClassName);
            if (table == null) {
                renewTable(clazz, indicatorField, initialIndicatorValue, indices);

                table = tables.get(tableClassName);
                if (waitForPreload) {
                    while (!table.isPreloaded()) {
                        try {
                            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                    logger.info("Inmemo completed to wait for preload " + tableClassName + '.');
                }
            } else {
                // Exactly the same class?
                if (table.getClazz().equals(clazz)) {
                    // logger.info("Exactly the same class [class=" + clazz + "].");
                    return;
                }

                String clazzSpec = ReflectionUtil.getTableClassSpec(clazz);
                // Compatible classes?
                if (table.getClazzSpec().equals(clazzSpec)) {
                    logger.info("Compatible classes " + tableClassName + " [class=" + clazz + "].");
                    return;
                }

                renewTable(clazz, indicatorField, initialIndicatorValue, indices);

                table = tables.get(tableClassName);
                if (waitForPreload) {
                    while (!table.isPreloaded()) {
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                    logger.info("Inmemo completed to wait for preload " + tableClassName + '.');
                }
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
    public static <T> void dropTableIfExists(@Nonnull Class<T> clazz) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);
        Table<? extends HasId> table = tables.get(tableClassName);

        if (table != null) {
            tablesLock.lock();
            try {
                tables.remove(tableClassName);
            } finally {
                tablesLock.unlock();
            }
        }
    }

    public static <T> int size(@Nonnull Class<T> clazz) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);
        Table<? extends HasId> table = tables.get(tableClassName);

        if (table != null) {
            return table.size();
        } else {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T> void waitForPreload(@Nonnull Class<T> clazz) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);
        Table<? extends HasId> table = tables.get(tableClassName);

        if (table != null) {
            boolean preloadedNow = false;
            while (!table.isPreloaded()) {
                preloadedNow = true;
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (preloadedNow) {
                logger.info("Inmemo completed to wait for preload " + tableClassName + '.');
            }
        } else {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }
    }

    private static <T extends HasId> void renewTable(Class<T> clazz,
                                                     String indicatorField, Object initialIndicatorValue,
                                                     Indices<T> indices) {
        Table<T> table = new Table<>(clazz, indicatorField);
        table.createUpdater(initialIndicatorValue);

        for (Index<T, ?> index : indices.getIndices()) {
            table.add(index);
        }

        for (RowListener rowListener : indices.getRowListeners()) {
            table.add(rowListener);
        }

        for (ItemListener<T> itemListener : indices.getItemListeners()) {
            table.add(itemListener);
        }

        table.runUpdater();
        tables.put(ReflectionUtil.getTableClassName(clazz), table);
    }

    /**
     * @param clazz           Table item class.
     * @param indexConstraint Index to use in search, index value.
     * @param <T>             Items class.
     * @return List of _copies_ of satisfying indexConstraint items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> List<T> find(
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint) {
        return find(clazz, indexConstraint, Inmemo.<T>acceptAnyMatcher());
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
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint,
            @Nonnull Matcher<T> matcher) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);

        Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        if (!table.isCompatibleItemClass(clazz)) {
            throw new InmemoException("Table class is incompatible with the given [tableClass=" + table.getClazz()
                    + ", clazz=" + clazz + "].");
        }

        //noinspection rawtypes
        Matcher tableMatcher = table.convertMatcher(clazz, matcher);

        //noinspection unchecked
        List<? extends HasId> result = table.find(indexConstraint, tableMatcher);

        if (result.isEmpty()) {
            return Collections.emptyList();
        } else {
            boolean sameClass = true;

            for (HasId hasId : result) {
                if (hasId != null) {
                    if (hasId.getClass() != clazz) {
                        sameClass = false;
                    }
                    break;
                }
            }

            if (false && sameClass) {
                //This case does not work with multiple classloaders.
                BeanCopier beanCopier = getBeanCopier(clazz, clazz);
                List<T> tableClassResult = new ArrayList<>(result.size());

                for (HasId tableItem : result) {
                    T item = ReflectionUtil.newInstance(clazz);
                    beanCopier.copy(tableItem, item, null);
                    tableClassResult.add(item);
                }

                return Collections.unmodifiableList(tableClassResult);
            } else {
                List<T> tableClassResult = new ArrayList<>(result.size());

                for (HasId tableItem : result) {
                    T item = ReflectionUtil.newInstance(clazz);
                    ReflectionUtil.copyProperties(tableItem, item);
                    tableClassResult.add(item);
                }

                return Collections.unmodifiableList(tableClassResult);
            }
        }
    }

    /**
     * @param throwOnNotUnique Throw exception if resulting item is not unique.
     * @param clazz            Table item class.
     * @param indexConstraint  Index to use in search, index value.
     * @param <T>              Items class.
     * @return List of _copies_ of satisfying indexConstraint items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> T findOnly(
            boolean throwOnNotUnique,
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint) {
        return findOnly(throwOnNotUnique, clazz, indexConstraint, Inmemo.<T>acceptAnyMatcher());
    }

    /**
     * @param throwOnNotUnique Throw exception if resulting item is not unique.
     * @param clazz            Table item class.
     * @param indexConstraint  Index to use in search, index value.
     * @param matcher          Predicate to choose items.
     * @param <T>              Items class.
     * @return List of _copies_ of matched items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> T findOnly(
            boolean throwOnNotUnique,
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint,
            @Nonnull Matcher<T> matcher) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);

        Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        if (!table.isCompatibleItemClass(clazz)) {
            throw new InmemoException("Table class is incompatible with the given [tableClass=" + table.getClazz()
                    + ", clazz=" + clazz + "].");
        }

        //noinspection rawtypes
        Matcher tableMatcher = table.convertMatcher(clazz, matcher);

        //noinspection unchecked
        HasId result = table.findOnly(throwOnNotUnique, indexConstraint, tableMatcher);

        if (result == null) {
            return null;
        }

        if (false && result.getClass() == clazz) {
            //This case does not work with multiple classloaders.
            T item = ReflectionUtil.newInstance(clazz);
            getBeanCopier(clazz, clazz).copy(result, item, null);
            return item;
        } else {
            T item = ReflectionUtil.newInstance(clazz);
            ReflectionUtil.copyProperties(result, item);
            return item;
        }
    }

    /**
     * @param clazz           Table item class.
     * @param indexConstraint Index to use in search, index value.
     * @param <T>             Items class.
     * @return Number of satisfying indexConstraint items.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> long findCount(
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint) {
        return findCount(clazz, indexConstraint, Inmemo.<T>acceptAnyMatcher());
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
            @Nonnull Class<T> clazz,
            @Nonnull IndexConstraint<?> indexConstraint,
            @Nonnull Matcher<T> matcher) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);

        Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }

        if (!table.isCompatibleItemClass(clazz)) {
            throw new InmemoException("Table class is incompatible with the given [tableClass=" + table.getClazz()
                    + ", clazz=" + clazz + "].");
        }

        //noinspection rawtypes
        Matcher tableMatcher = table.convertMatcher(clazz, matcher);

        //noinspection unchecked
        return table.findCount(indexConstraint, tableMatcher);
    }

    private static <T extends HasId> Table<? extends HasId> getTableByClass(Class<T> clazz) {
        String tableClassName = ReflectionUtil.getTableClassName(clazz);
        Table<? extends HasId> table = tables.get(tableClassName);
        if (table == null) {
            throw new InmemoException("Unable to find table for class name `" + tableClassName + "`.");
        }
        return table;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static <T extends HasId> void insertOrUpdate(@Nullable T object) {
        if (object == null) {
            return;
        }

        getTableByClass(object.getClass()).insertOrUpdate(object);
    }

    public static <T extends HasId> void update(Class<T> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("Illegal arguments for Inmemo#update: "
                    + "clazz = <null>");
        }

        getTableByClass(clazz).update();
    }

    public static <T extends HasId> void insertOrUpdateByIds(Class<T> clazz, Long... ids) {
        if (clazz == null) {
            throw new IllegalArgumentException("Illegal arguments for Inmemo#insertOrUpdateByIds: "
                    + "clazz = <null>");
        }
        if (ids == null) {
            throw new IllegalArgumentException("Illegal arguments for Inmemo#insertOrUpdateByIds: "
                    + "ids = <null>");
        }

        if (ids.length == 0) {
            return;
        }

        getTableByClass(clazz).insertOrUpdateByIds(ids);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void setDataSource(@Nonnull DataSource dataSource) {
        TableUpdater.setDataSource(dataSource);
    }

    private static final class ClassPair {
        private final Class<?> classA;
        private final Class<?> classB;

        private ClassPair(Class<?> classA, Class<?> classB) {
            this.classA = classA;
            this.classB = classB;
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClassPair classPair = (ClassPair) o;

            if (!classA.equals(classPair.classA)) return false;
            if (!classB.equals(classPair.classB)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = classA.hashCode();
            result = 31 * result + classB.hashCode();
            return result;
        }
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        BeanCopier beanCopier = getBeanCopier(Person.class, Person.class);
        Person from = null; // new Person(); // new Person(123, 124, 125, Person.A.Y);
        Person to = Person.class.newInstance();
        beanCopier.copy(from, to, null);
        System.out.println(to);
    }
}
