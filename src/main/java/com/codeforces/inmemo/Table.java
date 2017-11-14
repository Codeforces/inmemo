package com.codeforces.inmemo;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.log4j.Logger;
import org.jacuzzi.core.ArrayMap;
import org.jacuzzi.core.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.jacuzzi.core.ArrayMap.fromBinaryArray;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings("unused")
public class Table<T extends HasId> {
    private static final Logger logger = Logger.getLogger(Table.class);
    private static final Pattern INDICATOR_FIELD_SPLIT_PATTERN = Pattern.compile("@");
    private static final int JOURNAL_STREAM_BUFFER_SIZE = 128000000;

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
    private List<Row> journal = new ArrayList<>();
    private boolean useJournal = true;
    private static File journalsDir = new File(".");

    public static void setJournalsDir(File journalsDir) {
        if (!journalsDir.isDirectory()) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be a directory.");
        }

        try {
            PrintWriter statusWriter = new PrintWriter(new FileOutputStream(new File(journalsDir, "status")));
            statusWriter.println(new Date().toString());
            statusWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Journals directory '"
                    + journalsDir.getAbsolutePath() + "' expected to be writeable.", e);
        }

        Table.journalsDir = journalsDir;
    }

    Table(Class<T> clazz, String indicatorField) {
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

    @SuppressWarnings("unchecked")
    <U extends HasId> void insertOrUpdate(@Nonnull U item, @Nullable Row row) {
        Class<?> itemClass = item.getClass();
        String itemClassSpec = ReflectionUtil.getTableClassSpec(itemClass);

        if (clazzSpec.equals(itemClassSpec)) {
            T tableItem = ReflectionUtil.newInstance(clazz);
            ReflectionUtil.copyProperties(item, tableItem);
            internalInsertOrUpdate(tableItem, row);
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

    private void internalInsertOrUpdate(@Nonnull T item, @Nullable Row row) {
        lock.lock();
        try {
            if (journal != null && row != null) {
                journal.add(row);
            }

            ids.add(item.getId());
            for (Index<T, ?> index : indices.values()) {
                index.insertOrUpdate(item);
            }
            for (ItemListener<T> itemListener : itemListeners) {
                itemListener.insertOrUpdate(item);
            }
        } finally {
            lock.unlock();
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

    void writeJournal() throws IOException {
        if (useJournal && journal != null) {
            if (journal.isEmpty()) {
                logger.warn("Journal has not been dumped because of empty journal [table='" + clazz.getSimpleName() + "'].");
                return;
            }

            File journalFile = new File(journalsDir, clazz.getSimpleName() + ".inmemo");
            lock.lock();
            try {
                byte[] buffer = new byte[journal.size() * (journal.get(0).size() + 1) * 256];

                long startTimeMillis = System.currentTimeMillis();
                int offset = ArrayMap.toBinaryArray(buffer, 0, journal);
                long toBinaryArrayTimeMillis = System.currentTimeMillis() - startTimeMillis;
                logger.info("Journal binary data has been prepared in "
                        + toBinaryArrayTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + journal.size() + ", bytes=" + offset + "].");

                long beforeWriteTimeMillis = System.currentTimeMillis();
                OutputStream outputStream = new BufferedOutputStream(
                        new FileOutputStream(new File(journalsDir, clazz.getSimpleName() + ".inmemo")), JOURNAL_STREAM_BUFFER_SIZE);
                outputStream.write(buffer, 0, offset);
                outputStream.close();
                long writeTimeMillis = System.currentTimeMillis() - beforeWriteTimeMillis;
                logger.info("Journal binary data has been written in "
                        + writeTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + journal.size() + ", bytes=" + offset + "].");

                long durationTimeMillis = System.currentTimeMillis() - startTimeMillis;
                logger.info("Journal has been saved in "
                        + durationTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + journal.size() + ", bytes=" + offset + "].");

                journal = null;
            } finally {
                lock.unlock();
            }
        }
    }

    List<Row> readJournal() {
        if (!useJournal) {
            return null;
        }

        File journalFile = new File(journalsDir, clazz.getSimpleName() + ".inmemo");
        if (journalFile.isFile()) {
            long startTimeMillis = System.currentTimeMillis();
            InputStream inputStream = null;

            try {
                inputStream = new BufferedInputStream(
                                new FileInputStream(journalFile),
                                JOURNAL_STREAM_BUFFER_SIZE
                        );

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                int bufferSize = JOURNAL_STREAM_BUFFER_SIZE;
                byte[] buffer = new byte[bufferSize];
                while (true) {
                    int read = inputStream.read(buffer, 0, bufferSize);
                    if (read < 0) {
                        break;
                    }
                    byteArrayOutputStream.write(buffer, 0, read);
                }
                inputStream.close();
                long durationTimeMillis = System.currentTimeMillis() - startTimeMillis;
                byte[] bytes = byteArrayOutputStream.toByteArray();
                logger.info("Journal binary data has been read in "
                        + durationTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', bytes=" + bytes.length + "].");

                long beforeFfomBinaryArrayTimeMillis = System.currentTimeMillis();
                List<Row> rows = fromBinaryArray(bytes);

                durationTimeMillis = System.currentTimeMillis() - beforeFfomBinaryArrayTimeMillis;
                logger.info("Journal binary data has been parsed in "
                        + durationTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + rows.size() + "].");

                durationTimeMillis = System.currentTimeMillis() - startTimeMillis;
                logger.info("Journal has been read in "
                        + durationTimeMillis + " ms [table='" + clazz.getSimpleName()
                        + "', size=" + rows.size() + "].");

                return rows;
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
