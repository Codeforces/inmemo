package com.codeforces.inmemo;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.log4j.Logger;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
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

    private JournalWriter journalWriter;
    private boolean useJournal = true;
    private static File journalsDir = new File(".");
    private final AtomicInteger insertOrUpdateCount = new AtomicInteger();

    public static void setJournalsDir(File journalsDir) {
        if (!journalsDir.isDirectory()) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be a directory.");
        }

        try {
            PrintWriter statusWriter = new PrintWriter(Files.newOutputStream(new File(journalsDir, "status").toPath()));
            statusWriter.println(new Date());
            statusWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be writeable.", e);
        }

        Table.journalsDir = journalsDir;
    }

    static File getJournalsDirForTesting() {
        return journalsDir;
    }

    static void setJournalsDirForTesting(File journalsDir) {
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
        if (Inmemo.isJournalSupportUnset(clazz)) {
            useJournal = false;
            deleteStaleJournalFileQuietly();
        }
        if (useJournal) {
            journalWriter = createJournalWriter();
        }
    }

    void createUpdater(Object initialIndicatorValue) {
        setJournalEligible(initialIndicatorValue == null);
        tableUpdater = new TableUpdater<>(this, initialIndicatorValue);
    }

    void setJournalEligible(boolean journalEligible) {
        if (!useJournal) {
            journalWriter = null;
            return;
        }

        journalWriter = journalEligible ? createJournalWriter() : null;
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
            if (journalWriter != null && row != null) {
                journalWriter.addRow(row);
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

    void logBucketStats() {
        lock.lock();
        try {
            for (Index<T, ?> index : indices.values()) {
                Index.BucketStats bucketStats = index.getBucketStats();
                if (bucketStats == null) {
                    continue;
                }

                logger.info("Inmemo bucket stats [table="
                        + ReflectionUtil.getTableClassName(clazz)
                        + ", index="
                        + index.getName()
                        + ", buckets="
                        + bucketStats.getBucketCount()
                        + ", totalBucketSize="
                        + bucketStats.getTotalBucketSize()
                        + ", avgBucketSize="
                        + String.format(Locale.US, "%.2f", bucketStats.getAverageBucketSize())
                        + ", maxBucketSize="
                        + bucketStats.getMaxBucketSize()
                        + "].");
            }
        } finally {
            lock.unlock();
        }
    }

    void deleteJournal() throws IOException {
        File journalFile = new File(journalsDir, getInmemoFilename());
        deleteFile(journalFile);
        deleteFile(new File(journalsDir, getInmemoFilename() + ".tmp"));
    }

    private void deleteFile(File file) throws IOException {
        if (!file.isFile()) {
            return;
        }

        if (!file.delete()) {
            String message = "Journal has not been deleted [table='" + clazz.getSimpleName()
                    + "', file='" + file.getAbsolutePath() + "'].";
            logger.error(message);
            throw new IOException(message);
        }
    }

    private void deleteStaleJournalFileQuietly() {
        try {
            deleteJournal();
            logger.info("Journal support is disabled for table '" + clazz.getSimpleName()
                    + "', stale journal file (if any) has been deleted.");
        } catch (IOException | RuntimeException e) {
            logger.error("Journal support is disabled for table '" + clazz.getSimpleName()
                    + "', but failed to delete stale journal file.", e);
        }
    }

    private String getInmemoFilename() {
        return clazz.getSimpleName() + ".inmemo";
    }

    void writeJournal() throws IOException {
        if (useJournal && journalWriter != null) {
            lock.lock();
            try {
                journalWriter.finish();
                journalWriter = null;
            } finally {
                lock.unlock();
            }
        }
    }

    RowRoll readJournal() {
        if (!useJournal) {
            return null;
        }

        File journalFile = new File(journalsDir, getInmemoFilename());
        try {
            return JournalReader.readAll(journalFile, clazz, clazzSpec);
        } catch (Exception e) {
            logger.error("Unexpected exception while reading journal [table='" + clazz.getSimpleName() + "'].", e);
            return null;
        }
    }

    boolean isUseJournal() {
        return useJournal;
    }

    /* init. */ {
        if (!"true".equals(System.getProperty("Inmemo.UseJournal"))) {
            useJournal = false;
        }
    }

    private JournalWriter createJournalWriter() {
        return new JournalWriter(new File(journalsDir, getInmemoFilename()), clazz, clazzSpec);
    }
}
