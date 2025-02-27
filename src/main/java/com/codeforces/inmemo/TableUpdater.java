package com.codeforces.inmemo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jacuzzi.core.Jacuzzi;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;
import org.jacuzzi.core.TypeOracle;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
class TableUpdater<T extends HasId> {
    private static final Logger logger = Logger.getLogger(TableUpdater.class);

    private static final List<Thread> tableUpdaterThreads
            = Collections.synchronizedList(new ArrayList<>());

    private static final AtomicInteger tableUpdaterThreadCount
            = new AtomicInteger(0);

    /**
     * Delay after meaningless update try.
     */
    private static final long RESCAN_TIME_MILLIS = 750;

    private static final int MAX_ROWS_IN_SINGLE_SQL_STATEMENT = 2_000_000;
    private static final int MAX_UPDATE_SAME_INDICATOR_TIMES = 10;

    private final Lock updateLock = new ReentrantLock();

    private static DataSource dataSource;
    private static final Map<String, DataSource> dataSourceByClazzName = new ConcurrentHashMap<>();
    private static final Collection<TableUpdater<? extends HasId>> instances
            = Collections.synchronizedList(new ArrayList<>());

    private final Table<?> table;
    private final Thread thread;
    private final String threadName;
    private volatile boolean running;

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;

    private final AtomicReference<Object> lastIndicatorValue = new AtomicReference<>();
    private final long startTimeMillis;

    private final Map<Long, Integer> lastEntityIdsUpdateCount = new ConcurrentHashMap<>();

    TableUpdater(Table<T> table, Object initialIndicatorValue) {
        if (dataSource == null) {
            logger.error("It should be called static Inmemo#setDataSource()"
                    + " before any instance of TableUpdater.");
            throw new InmemoException("It should be called static Inmemo#setDataSource()"
                    + " before any instance of TableUpdater.");
        }

        this.table = table;
        this.lastIndicatorValue.set(initialIndicatorValue);

        DataSource clazzDataSource = dataSourceByClazzName.get(table.getClazz().getName());
        jacuzzi = Jacuzzi.getJacuzzi(clazzDataSource == null ? dataSource : clazzDataSource);

        typeOracle = TypeOracle.getTypeOracle(table.getClazz());

        threadName = "InmemoUpdater#" + table.getClazz();
        thread = new Thread(new TableUpdaterRunnable(this), threadName);
        thread.setDaemon(true);
        tableUpdaterThreads.add(thread);

        logger.info("Started Inmemo table updater thread '" + threadName + "'.");
        startTimeMillis = System.currentTimeMillis();

        //noinspection ThisEscapedInObjectConstruction
        instances.add(this);
    }

    @SuppressWarnings("UnusedDeclaration")
    static void setDataSource(DataSource dataSource) {
        TableUpdater.dataSource = dataSource;
    }

    @SuppressWarnings("UnusedDeclaration")
    static void stop() {
        synchronized (instances) {
            for (TableUpdater<? extends HasId> instance : instances) {
                instance.running = false;
            }
        }
    }

    void start() {
        running = true;
        thread.start();
    }

    private int getMaxUpdateSameIndicatorTimes() {
        if (table.isPreloaded()) {
            return MAX_UPDATE_SAME_INDICATOR_TIMES;
        } else {
            return 1;
        }
    }

    void insertOrUpdateById(Long id) {
        if (id == null) {
            return;
        }

        RowRoll rows = jacuzzi.findRowRoll("SELECT * FROM "
                + typeOracle.getTableName()
                + " WHERE "
                + typeOracle.getIdColumn()
                + " = "
                + id);

        if (rows == null || rows.isEmpty()) {
            return;
        }

        if (rows.size() == 1) {
            Row row = rows.getRow(0);
            T entity = typeOracle.convertFromRow(row);

            table.insertOrUpdate(entity, row);
            table.insertOrUpdate(row);
        } else {
            throw new InmemoException("Expected at most one item of "
                    + table.getClazz() + " with id = " + id + '.');
        }
    }

    List<T> findAndUpdateByEmergencyQueryFields(Object[] fields) {
        validateFieldsArray(fields);

        String[] fieldNames = new String[fields.length / 2];
        Object[] fieldValues = new Object[fields.length / 2];

        for (int index = 0; index < fields.length; index += 2) {
            fieldNames[index / 2] = (String) fields[index];
            fieldValues[index / 2] = fields[index + 1];
        }

        String formattedFields = typeOracle.getQueryFindSql(fieldNames);

        RowRoll rows = jacuzzi.findRowRoll("SELECT * FROM "
                + typeOracle.getTableName()
                + " WHERE "
                + formattedFields
                + " ORDER BY "
                + typeOracle.getIdColumn(), fieldValues);

        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        logger.warn("Emergency case: found "
                + rows.size()
                + " items of class "
                + table.getClazz().getName()
                + " [fields="
                + formattedFields
                + "].");

        List<T> result = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.getRow(i);

            T entity = typeOracle.convertFromRow(row);
            logger.warn("Emergency found: "
                    + table.getClazz().getName()
                    + " id="
                    + entity.getId()
                    + " [fields="
                    + formattedFields
                    + "].");

            result.add(entity);

            table.insertOrUpdate(entity, row);
            table.insertOrUpdate(row);
        }

        return result;
    }

    private static void validateFieldsArray(Object[] fields) {
        if (fields.length % 2 != 0) {
            throw new IllegalArgumentException("EmergencyQueryFields array should have"
                    + " even length. Found: "
                    + Arrays.toString(fields) + '.');
        }

        for (int index = 0; index < fields.length; index += 2) {
            Object field = fields[index];
            if (!(field instanceof String) || ((String) field).isEmpty()) {
                throw new IllegalArgumentException("EmergencyQueryFields array must contain"
                        + " non-empty strings on even positions. Found: "
                        + Arrays.toString(fields) + '.');
            }
        }
    }

    void update() {
        if (running) {
            internalUpdate();
        }
    }

    private void randomizedUpdate() {
        List<Long> updatedIds = internalUpdate();
        sleepBetweenRescans(updatedIds.size());
    }

    private List<Long> internalUpdate() {
        updateLock.lock();

        try {
            long startTimeMillis = System.currentTimeMillis();
            Object prevLastIndicatorValue = lastIndicatorValue.get();
            RowRoll rows = getRecentlyChangedRows(prevLastIndicatorValue);

            long afterGetRecentlyChangedRowsMillis = System.currentTimeMillis();
            long getRecentlyChangedMillis = afterGetRecentlyChangedRowsMillis - startTimeMillis;

            if (rows.size() >= 100
                    || getRecentlyChangedMillis >= TimeUnit.SECONDS.toMillis(1)) {
                logger.error("Table '"
                        + table.getClazz().getSimpleName()
                        + "': getRecentlyChangedRows returns "
                        + rows.size()
                        + " rows [prevLastIndicatorValue="
                        + prevLastIndicatorValue
                        + ", lastIndicatorValue="
                        + lastIndicatorValue.get()
                        + ", thread="
                        + threadName
                        + ", time=" + getRecentlyChangedMillis + " ms].");
            }

            boolean hasInsertOrUpdateByRow = table.hasInsertOrUpdateByRow();
            List<Long> updatedIds = new ArrayList<>();

            int idColumn = getIdColumn(rows);
            int indicatorFieldColumn = rows.getColumn(table.getIndicatorField());

            for (int i = 0; i < rows.size(); i++) {
                long id = (long) rows.getValue(i, idColumn);

                if (Objects.equals(rows.getValue(i, indicatorFieldColumn), prevLastIndicatorValue)
                        && lastEntityIdsUpdateCount.containsKey(id)
                        && lastEntityIdsUpdateCount.get(id) >= getMaxUpdateSameIndicatorTimes()) {
                    continue;
                }

                // Insert or update entity.
                Row row = rows.getRow(i);
                T entity = typeOracle.convertFromRow(row);
                table.insertOrUpdate(entity, row);

                updatedIds.add(id);

                // Insert or update row.
                if (hasInsertOrUpdateByRow) {
                    table.insertOrUpdate(row);
                }

                if (i > 0 && i % 100000 == 0) {
                    logger.warn("Inserted "
                            + i
                            + " rows in a batch [table="
                            + table.getClazz().getSimpleName()
                            + "].");
                }

                if (table.hasSize()) {
                    int tableSize = table.size();
                    if (tableSize > 0 && tableSize % 100000 == 0) {
                        logger.warn("Table "
                                + table.getClazz().getSimpleName()
                                + " contains now "
                                + tableSize
                                + " rows.");
                    }
                }

                lastIndicatorValue.set(row.get(table.getIndicatorField()));
            }

            if (updatedIds.size() >= 10) {
                logger.info("Thread '"
                        + threadName
                        + "' has found "
                        + rows.size()
                        + "("
                        + updatedIds.size()
                        + ") rows to update in "
                        + getRecentlyChangedMillis
                        + " ms [prevLastIndicatorValue="
                        + prevLastIndicatorValue
                        + ", lastIndicatorValue="
                        + lastIndicatorValue.get()
                        + "].");

                if (updatedIds.size() <= 100) {
                    StringBuilder ids = new StringBuilder();
                    for (long id : updatedIds) {
                        if (ids.length() > 0) {
                            ids.append(',');
                        }
                        ids.append(id);
                    }
                    logger.info("Updated entries have id=" + ids + '.');
                }

                logger.info("Thread '"
                        + threadName
                        + "' has updated "
                        + updatedIds.size()
                        + " items in "
                        + (System.currentTimeMillis() - startTimeMillis)
                        + " ms [prevLastIndicatorValue="
                        + prevLastIndicatorValue
                        + ", lastIndicatorValue="
                        + lastIndicatorValue.get()
                        + "].");
            }

            if (updatedIds.isEmpty() && !table.isPreloaded()) {
                if (table.hasSize()) {
                    logger.info("Inmemo ready to dump journal of table " + ReflectionUtil.getTableClassName(table.getClazz())
                            + " [items=" + table.size() + "].");
                }
                long totalTimeMillis = System.currentTimeMillis() - this.startTimeMillis;
                try {
                    table.writeJournal();
                } catch (IOException e) {
                    logger.error("Inmemo failed to dump journal of table " + ReflectionUtil.getTableClassName(table.getClazz())
                            + " [items=" + table.size() + "] in " + totalTimeMillis + " ms.", e);
                }
                if (table.hasSize()) {
                    logger.log(totalTimeMillis < TimeUnit.SECONDS.toMillis(1) ? Level.INFO : Level.WARN, "Inmemo preloaded " + ReflectionUtil.getTableClassName(table.getClazz())
                            + " [items=" + table.size() + "] in " + totalTimeMillis + " ms.");
                } else {
                    logger.log(totalTimeMillis < TimeUnit.SECONDS.toMillis(1) ? Level.INFO : Level.WARN, "Inmemo preloaded " + ReflectionUtil.getTableClassName(table.getClazz())
                            + " in " + totalTimeMillis + " ms.");
                }
                table.setPreloaded(true);
            }

            Object newLastIndicatorValue = lastIndicatorValue.get();
            if (!Objects.equals(prevLastIndicatorValue, newLastIndicatorValue)) {
                lastEntityIdsUpdateCount.clear();
            }
            List<Long> trulyUpdatedIds = new ArrayList<>(updatedIds.size());

            for (int i = 0; i < rows.size(); i++) {
                if (Objects.equals(rows.getValue(i, indicatorFieldColumn), newLastIndicatorValue)) {
                    long id = (long) rows.getValue(i, idColumn);
                    Integer updateCount = lastEntityIdsUpdateCount.get(id);
                    if (updateCount == null) {
                        trulyUpdatedIds.add(id);
                        updateCount = 1;
                    } else {
                        updateCount += 1;
                    }
                    lastEntityIdsUpdateCount.put(id, updateCount);
                }
            }

            return trulyUpdatedIds;
        } finally {
            updateLock.unlock();
        }
    }

    private static int getIdColumn(RowRoll rowRoll) {
        int column = rowRoll.getColumn("id");
        if (column == -1) {
            column = rowRoll.getColumn("ID");
        }
        return column;
    }

    private void sleepBetweenRescans(int updatedCount) {
        long rescanTimeMillis = getRescanTimeMillis();

        if (updatedCount == 0) {
            sleep((4 * rescanTimeMillis / 5)
                    + ThreadLocalRandom.current().nextInt((int) (rescanTimeMillis / 5)));
        } else if (updatedCount * 2 > MAX_ROWS_IN_SINGLE_SQL_STATEMENT) {
            logger.info("Thread '"
                    + threadName
                    + "' will not sleep because it updated near maximum row count.");
        } else {
            sleep(ThreadLocalRandom.current().nextInt((int) (rescanTimeMillis / 5)));
        }
    }

    private long getRescanTimeMillis() {
        return RESCAN_TIME_MILLIS;
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            logger.error("Thread '" + threadName + "' has been stopped because of InterruptedException.", e);
            running = false;
        }
    }

    private RowRoll getRecentlyChangedRows(Object indicatorLastValue) {
        long startTimeMillis = System.currentTimeMillis();
        RowRoll rows = null;

        if (lastIndicatorValue.get() == null && table.isUseJournal()) {
            RowRoll journalRows = table.readJournal();
            if (journalRows != null && !journalRows.isEmpty()) {
                rows = journalRows;
            }
        }

        if (rows != null) {
            logger.info("getRecentlyChangedRows loads data of using the journal in "
                    + (System.currentTimeMillis() - startTimeMillis)
                    + " ms [table=" + table.getClazz().getSimpleName() + "].");
            return rows;
        }

        String forceIndexClause = table.getDatabaseIndex() == null ? "" : ("FORCE INDEX (" + table.getDatabaseIndex() + ')');

        if (indicatorLastValue == null) {
            rows = jacuzzi.findRowRoll("SELECT * FROM "
                    + typeOracle.getTableName()
                    + ' '
                    + forceIndexClause
                    + " ORDER BY "
                    + table.getIndicatorField()
                    + ", "
                    + typeOracle.getIdColumn()
                    + " LIMIT "
                    + MAX_ROWS_IN_SINGLE_SQL_STATEMENT);
        } else {
            rows = jacuzzi.findRowRoll("SELECT * FROM "
                            + typeOracle.getTableName()
                            + ' '
                            + forceIndexClause
                            + " WHERE "
                            + table.getIndicatorField()
                            + " >= ? ORDER BY "
                            + table.getIndicatorField()
                            + ", "
                            + typeOracle.getIdColumn()
                            + " LIMIT "
                            + MAX_ROWS_IN_SINGLE_SQL_STATEMENT,
                    indicatorLastValue
            );
        }

        long queryTimeMillis = System.currentTimeMillis() - startTimeMillis;
        if (queryTimeMillis * 10 > getRescanTimeMillis()) {
            logger.warn("Rescanning query for entity `"
                    + table.getClazz().getName()
                    + "` took too long time "
                    + queryTimeMillis
                    + " ms.");
        }
        return rows;
    }

    @SuppressWarnings("WeakerAccess")
    public static void setSpecificDataSource(Class<?> clazz, DataSource dataSource) {
        String clazzName = clazz.getName();
        logger.info("Setting specific data source for [clazz=" + clazzName + "].");
        dataSourceByClazzName.put(clazzName, dataSource);
    }

    private static final class TableUpdaterRunnable implements Runnable {
        private final TableUpdater<?> tableUpdater;

        private TableUpdaterRunnable(TableUpdater<?> tableUpdater) {
            this.tableUpdater = tableUpdater;
        }

        @Override
        public void run() {
            tableUpdaterThreadCount.incrementAndGet();
            while (tableUpdater.running) {
                try {
                    tableUpdater.randomizedUpdate();
                } catch (Exception e) {
                    logger.error("Unexpected "
                            + e.getClass().getName()
                            + " exception in TableUpdaterRunnable of "
                            + tableUpdater.threadName
                            + ": " + e, e);
                }
            }

            tableUpdaterThreadCount.decrementAndGet();

            logger.warn("Inmemo update thread for "
                    + tableUpdater.table.getClazz().getName()
                    + " finished.");
        }
    }

    private static final class TableUpdaterThreadsPrinterRunnable implements Runnable {
        @SuppressWarnings("BusyWait")
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(300));
                } catch (InterruptedException e) {
                    logger.warn("TableUpdaterThreadsPrinterRunnable stopped because of"
                            + " InterruptedException.");
                    break;
                }

                if (tableUpdaterThreadCount.get() == 0) {
                    logger.warn("TableUpdaterThreadsPrinterRunnable stopped because of"
                            + " `tableUpdaterThreadCount.get() == 0`.");
                    break;
                }

                synchronized (tableUpdaterThreads) {
                    logger.info("tableUpdaterThreads.size()=" + tableUpdaterThreads.size() + ".");
                    for (Thread tableUpdaterRunnable : tableUpdaterThreads) {
                        printStackTrace(tableUpdaterRunnable);
                    }
                }
            }
        }

        private void printStackTrace(Thread thread) {
            StringBuilder result = new StringBuilder();
            StackTraceElement[] elements = thread.getStackTrace();
            for (StackTraceElement element : elements) {
                result.append(thread.getName()).append(": ").append(element.toString()).append('\n');
            }
            result.append("\n\n");
            logger.info(thread.getName() + " stack trace: " + result);
        }
    }

    static {
        Thread tableUpdaterThreadsPrinterThread = new Thread(new TableUpdaterThreadsPrinterRunnable());
        tableUpdaterThreadsPrinterThread.setDaemon(true);
        tableUpdaterThreadsPrinterThread.setUncaughtExceptionHandler((t, e)
                -> logger.error("Uncaught exception [thread=" + t + ", exception=" + e + "]."));
        tableUpdaterThreadsPrinterThread.start();
    }
}
