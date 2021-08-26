package com.codeforces.inmemo;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.log4j.Logger;
import org.jacuzzi.core.ArrayMap;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings("unused")
public class Table<T extends HasId> {
    private static final Logger logger = Logger.getLogger(Table.class);
    private static final Pattern INDICATOR_FIELD_SPLIT_PATTERN = Pattern.compile("@");
    private static final int JOURNAL_STREAM_BUFFER_SIZE = 4 * 1024 * 1024;

    private final Lock lock = new ReentrantLock();

    private final Map<String, Index<T, ?>> indices = new ConcurrentHashMap<>();
    private final List<RowListener> rowListeners = new ArrayList<>();
    private final List<ItemListener<T>> itemListeners = new ArrayList<>();

    private final Class<T> clazz;
    private final String clazzSpec;
    private final String indicatorField;
    private final String databaseIndex;
    private final Inmemo.Filter<T> rowFilter;

    private TableUpdater<T> tableUpdater;
    private volatile boolean preloaded;
    private final TLongSet ids;

    private RowRoll journal = new RowRoll();
    private boolean useJournal = true;
    private static File journalsDir = new File(".");
    private final AtomicInteger insertOrUpdateCount = new AtomicInteger();

    public static void setJournalsDir(File journalsDir) {
        if (!journalsDir.isDirectory()) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be a directory.");
        }

        try {
            PrintWriter statusWriter = new PrintWriter(new FileOutputStream(new File(journalsDir, "status")));
            statusWriter.println(new Date());
            statusWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be writeable.", e);
        }

        Table.journalsDir = journalsDir;
    }

    Table(Class<T> clazz, String indicatorField, Inmemo.Filter<T> rowFilter) {
        this.clazz = clazz;
        if (indicatorField.contains("@")) {
            String[] tokens = INDICATOR_FIELD_SPLIT_PATTERN.split(indicatorField);
            this.indicatorField = tokens[0];
            this.databaseIndex = tokens[1];
        } else {
            this.indicatorField = indicatorField;
            this.databaseIndex = null;
        }
        clazzSpec = ReflectionUtil.getTableClassSpec(clazz);
        ids = Inmemo.getNoSizeSupportClasses().contains(clazz) ? null : new TLongHashSet();
        this.rowFilter = rowFilter;
    }

    void createUpdater(Object initialIndicatorValue) {
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

    @SuppressWarnings("SameParameterValue")
    void setPreloaded(boolean preloaded) {
        this.preloaded = preloaded;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isCompatibleItemClass(Class<? extends HasId> otherItemClass) {
        return clazz == otherItemClass
                || clazzSpec.equals(ReflectionUtil.getTableClassSpec(otherItemClass));
    }

    @SuppressWarnings("unchecked")
    <U extends HasId> Matcher<T> convertMatcher(final Class<U> otherClass, final Matcher<U> otherMatcher) {
        if (clazz == otherClass) {
            return (Matcher<T>) otherMatcher;
        }

        String otherClassSpec = ReflectionUtil.getTableClassSpec(otherClass);
        if (clazzSpec.equals(otherClassSpec)) {
            return tableItem -> {
                U otherItem = ReflectionUtil.newInstance(otherClass);
                ReflectionUtil.copyProperties(tableItem, otherItem);
                return otherMatcher.match(otherItem);
            };
        }

        throw new InmemoException(String.format(
                "Can't convert matchers because the are incompatible [class=%s, otherClass=%s].",
                clazz, otherClass
        ));
    }

    <V> void add(Index<T, V> index) {
        indices.put(index.getName(), index);
        index.setTable(this);
    }

    void add(RowListener rowListener) {
        rowListeners.add(rowListener);
    }

    void add(ItemListener<T> itemListener) {
        itemListeners.add(itemListener);
    }

    boolean hasInsertOrUpdateByRow() {
        return !rowListeners.isEmpty();
    }

    boolean hasSize() {
        return ids != null;
    }

    <U extends HasId> void insertOrUpdate(@Nonnull U item, @Nullable Row row) {
        Class<?> itemClass = item.getClass();
        String itemClassSpec = ReflectionUtil.getTableClassSpec(itemClass);

        if (clazzSpec.equals(itemClassSpec)) {
            T tableItem = ReflectionUtil.newInstance(clazz);
            ReflectionUtil.copyProperties(item, tableItem);
            if (rowFilter == null || (rowFilter.testItem(tableItem) && (row == null || rowFilter.testRow(row)))) {
                internalInsertOrUpdate(tableItem, row);
            }
        } else {
            throw new InmemoException("Table class is incompatible with the class of object [tableClass=" + clazz
                    + ", clazz=" + itemClass + "].");
        }
    }

    void update() {
        if (tableUpdater != null) {
            tableUpdater.update();
        }
    }

    void insertOrUpdate(Row row) {
        if (rowFilter == null || rowFilter.testRow(row)) {
            for (RowListener rowListener : rowListeners) {
                rowListener.insertOrUpdate(row);
            }
        }
    }

    int size() {
        if (ids == null) {
            throw new UnsupportedOperationException("The operation is unsupported due Inmemo.unsetSizeSupport(clazz).");
        }

        lock.lock();
        try {
            return ids.size();
        } finally {
            lock.unlock();
        }
    }

    private void internalInsertOrUpdate(@Nonnull T item, @Nullable Row row) {
        lock.lock();
        try {
            if (journal != null && row != null) {
                journal.addRow(row);
            }

            if (ids != null) {
                ids.add(item.getId());
            }

            for (Index<T, ?> index : indices.values()) {
                index.insertOrUpdate(item);
            }
            for (ItemListener<T> itemListener : itemListeners) {
                itemListener.insertOrUpdate(item);
            }
        } finally {
            lock.unlock();
        }

        int count = insertOrUpdateCount.incrementAndGet();
        if (count % 100000 == 0) {
            logger.info("Inmemo: table " + ReflectionUtil.getTableClassName(getClazz())
                    + " insertOrUpdateCount=" + count + ".");
        }
    }

    List<T> find(IndexConstraint<?> indexConstraint, Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.find(indexConstraint.getValue(), predicate);
    }

    T findOnly(boolean throwOnNotUnique, IndexConstraint<?> indexConstraint, Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.findOnly(throwOnNotUnique, indexConstraint.getValue(), predicate);
    }

    long findCount(IndexConstraint<?> indexConstraint, Matcher<T> predicate) {
        if (indexConstraint == null) {
            throw new InmemoException("Nonnul IndexConstraint is required [tableClass="
                    + ReflectionUtil.getTableClassName(clazz) + "].");
        }

        Index<T, ?> index = indices.get(indexConstraint.getIndexName());
        if (index == null) {
            throw new IllegalArgumentException("Unexpected index name `" + indexConstraint.getIndexName() + "`.");
        }

        return index.findCount(indexConstraint.getValue(), predicate);
    }

    void insertOrUpdateByIds(Long[] ids) {
        for (Long id : ids) {
            if (id == null) {
                continue;
            }

            tableUpdater.insertOrUpdateById(id);
        }
    }

    List<T> findAndUpdateByEmergencyQueryFields(Object[] fields) {
        return tableUpdater.findAndUpdateByEmergencyQueryFields(fields);
    }

    void deleteJournal() throws IOException {
        File journalFile = new File(journalsDir, clazz.getSimpleName() + ".inmemo");
        if (journalFile.isFile()) {
            if (!journalFile.delete()) {
                String message = "Journal has not been deleted [table='" + clazz.getSimpleName() + "'].";
                logger.error(message);
                throw new IOException(message);
            }
        }
    }

    void writeJournal() throws IOException {
        if (useJournal && journal != null) {
            if (journal.isEmpty()) {
                logger.warn("Journal has not been dumped because of empty journal [table='" + clazz.getSimpleName() + "'].");
                return;
            }

            File journalFile = new File(journalsDir, clazz.getSimpleName() + ".inmemo");
            lock.lock();
            try {
                long startTimeMillis = System.currentTimeMillis();
                try (OutputStream outputStream = new BufferedOutputStream(
                        new FileOutputStream(journalFile), JOURNAL_STREAM_BUFFER_SIZE)) {
                    ArrayMap.writeRowRoll(outputStream, journal);
                }
                long writeRowRollTimeMillis = System.currentTimeMillis() - startTimeMillis;
                logger.info("Journal binary data has been prepared and written in "
                        + writeRowRollTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + journal.size() + ", bytes=" + journalFile.length() + "].");
                journal = null;
            } finally {
                lock.unlock();
            }
        }
    }

    RowRoll readJournal() {
        if (!useJournal) {
            return null;
        }

        File journalFile = new File(journalsDir, clazz.getSimpleName() + ".inmemo");
        if (journalFile.isFile()) {
            long startTimeMillis = System.currentTimeMillis();
            InputStream inputStream = null;
            try {
                inputStream = new BufferedInputStream(new FileInputStream(journalFile), JOURNAL_STREAM_BUFFER_SIZE);
                RowRoll rowRoll = ArrayMap.readRowRoll(inputStream);
                long durationTimeMillis = System.currentTimeMillis() - startTimeMillis;
                logger.info("Journal binary data has been read and parsed in "
                        + durationTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', bytes=" + journalFile.length() + "].");
                return rowRoll;
            } catch (ClassCastException e) {
                logger.error("ClassCastException: Unable to read journal [table='" + clazz.getSimpleName() + "'].", e);
                return null;
            } catch (IOException e) {
                logger.error("IOException: Unable to read journal [table='" + clazz.getSimpleName() + "'].", e);
                return null;
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        logger.error("Unable to close journal after reading [table='" + clazz.getSimpleName() + "'].", e);
                        // No operations.
                    }
                }
            }
        } else {
            logger.info("Can't read journal because of no file '" + journalFile + "'.");
        }

        return null;
    }

    boolean isUseJournal() {
        return useJournal;
    }

    /* init. */ {
        if (!"true".equals(System.getProperty("Inmemo.UseJournal"))) {
            journal = null;
            useJournal = false;
        }
    }
}
