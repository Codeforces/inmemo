package com.codeforces.inmemo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.jacuzzi.core.Jacuzzi;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.RowRoll;
import org.jacuzzi.core.TypeOracle;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
class TableUpdater<T extends HasId> {
    private static final Logger logger = Logger.getLogger(TableUpdater.class);

    /**
     * Delay after meaningless update try.
     */
    private static final long RESCAN_TIME_MILLIS = 750;

    private static final int MAX_ROWS_IN_SINGLE_SQL_STATEMENT = 2_000_000;
    private static final int MAX_UPDATE_SAME_INDICATOR_TIMES = 5;

    private final Lock updateLock = new ReentrantLock();

    private static DataSource dataSource;
    private static Map<String, DataSource> dataSourceByClazzName = new ConcurrentHashMap<>();
    private static final Collection<TableUpdater<? extends HasId>> instances = new ArrayList<>();

    private final Table table;
    private final Thread thread;
    private final String threadName;
    private volatile boolean running;

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;

    private Object lastIndicatorValue;
    private final long startTimeMillis;

    private final Map<Long, Integer> lastEntityIdsUpdateCount = new HashMap<>();

    TableUpdater(Table<T> table, Object initialIndicatorValue) {
        if (dataSource == null) {
            logger.error("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
            throw new InmemoException("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
        }

        this.table = table;
        this.lastIndicatorValue = initialIndicatorValue;

        DataSource clazzDataSource = dataSourceByClazzName.get(table.getClazz().getName());
        jacuzzi = Jacuzzi.getJacuzzi(clazzDataSource == null ? dataSource : clazzDataSource);

        typeOracle = TypeOracle.getTypeOracle(table.getClazz());

        threadName = "InmemoUpdater#" + table.getClazz();
        thread = new Thread(new TableUpdaterRunnable(), threadName);
        thread.setDaemon(true);

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
        for (TableUpdater<? extends HasId> instance : instances) {
            instance.running = false;
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
        RowRoll rows = jacuzzi.findRowRoll(String.format("SELECT * FROM %s WHERE %s = %s",
                typeOracle.getTableName(), typeOracle.getIdColumn(), id.toString()
        ));

        if (rows == null || rows.isEmpty()) {
            return;
        }

        if (rows.size() == 1) {
            Row row = rows.getRow(0);
            T entity = typeOracle.convertFromRow(row);

            table.insertOrUpdate(entity, row);
            table.insertOrUpdate(row);
        } else {
            throw new InmemoException("Expected at most one item of " + table.getClazz() + " with id = " + id + '.');
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

        RowRoll rows = jacuzzi.findRowRoll(String.format("SELECT * FROM %s WHERE %s ORDER BY %s",
                typeOracle.getTableName(), formattedFields, typeOracle.getIdColumn()), fieldValues);

        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        logger.warn("Emergency case: found " + rows.size() + " items of class " + table.getClazz().getName() + " [fields=" + formattedFields + "].");

        List<T> result = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.getRow(i);

            T entity = typeOracle.convertFromRow(row);
            logger.warn("Emergency found: " + table.getClazz().getName() + " id=" + entity.getId() + " [fields=" + formattedFields + "].");

            result.add(entity);

            table.insertOrUpdate(entity, row);
            table.insertOrUpdate(row);
        }

        return result;
    }

    private static void validateFieldsArray(Object[] fields) {
        if (fields.length % 2 != 0) {
            throw new IllegalArgumentException("EmergencyQueryFields array should have even length. Found: "
                    + Arrays.toString(fields) + '.');
        }

        for (int index = 0; index < fields.length; index += 2) {
            Object field = fields[index];
            if (!(field instanceof String) || ((String) field).isEmpty()) {
                throw new IllegalArgumentException("EmergencyQueryFields array must contain non-empty strings on even positions. Found: "
                        + Arrays.toString(fields) + '.');
            }
        }
    }

    void update() {
        if (running) {
            internalUpdate();
        }
    }

    private void update(Random timeSleepRandom) {
        List<Long> updatedIds = internalUpdate();
        sleepBetweenRescans(timeSleepRandom, updatedIds.size());
    }

    private List<Long> internalUpdate() {
        updateLock.lock();

        try {
            long startTimeMillis = System.currentTimeMillis();
            RowRoll rows = getRecentlyChangedRows(lastIndicatorValue);

            long afterGetRecentlyChangedRowsMillis = System.currentTimeMillis();
            long getRecentlyChangedMillis = afterGetRecentlyChangedRowsMillis - startTimeMillis;

            if (rows.size() >= 100 || getRecentlyChangedMillis >= TimeUnit.SECONDS.toMillis(1)) {
                logger.error("Table '" + table.getClazz().getSimpleName() + "': getRecentlyChangedRows returns " + rows.size()
                        + " rows [lastIndicatorValue=" + lastIndicatorValue
                        + ", thread=" + threadName
                        + ", time=" + getRecentlyChangedMillis + " ms].");
            }

            Object previousIndicatorLastValue = lastIndicatorValue;
            boolean hasInsertOrUpdateByRow = table.hasInsertOrUpdateByRow();
            List<Long> updatedIds = new ArrayList<>();

            int idColumn = getIdColumn(rows);
            int indicatorFieldColumn = rows.getColumn(table.getIndicatorField());

            for (int i = 0; i < rows.size(); i++) {
                long id = (long) rows.getValue(i, idColumn);

                if (Objects.equals(rows.getValue(i, indicatorFieldColumn), previousIndicatorLastValue)
                        && lastEntityIdsUpdateCount.containsKey(id) && lastEntityIdsUpdateCount.get(id) >= getMaxUpdateSameIndicatorTimes()) {
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
                    logger.warn("Inserted " + i + " rows in a batch [table=" + table.getClazz().getSimpleName() + "].");
                }

                if (table.hasSize()) {
                    int tableSize = table.size();
                    if (tableSize > 0 && tableSize % 100000 == 0) {
                        logger.warn("Table " + table.getClazz().getSimpleName() + " contains now " + tableSize + " rows.");
                    }
                }

                lastIndicatorValue = row.get(table.getIndicatorField());
            }

            if (updatedIds.size() >= 10) {
                logger.info(String.format("Thread '%s' has found %s rows to update in %d ms [lastIndicatorValue=" + lastIndicatorValue + "].", threadName,
                        rows.size(), getRecentlyChangedMillis));

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

                if (!updatedIds.isEmpty()) {
                    logger.info(String.format("Thread '%s' has updated %d items in %d ms [lastIndicatorValue=" + lastIndicatorValue + "].",
                            threadName, updatedIds.size(), System.currentTimeMillis() - startTimeMillis));
                }
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

            if (!Objects.equals(previousIndicatorLastValue, lastIndicatorValue)) {
                lastEntityIdsUpdateCount.clear();
            }
            List<Long> trulyUpdatedIds = new ArrayList<>(updatedIds.size());

            for (int i = 0; i < rows.size(); i++) {
                if (Objects.equals(rows.getValue(i, indicatorFieldColumn), lastIndicatorValue)) {
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

    private void sleepBetweenRescans(Random timeSleepRandom, int updatedCount) {
        long rescanTimeMillis = getRescanTimeMillis();

        if (updatedCount == 0) {
            sleep((4 * rescanTimeMillis / 5) + timeSleepRandom.nextInt((int) (rescanTimeMillis / 5)));
        } else if ((updatedCount << 1) > MAX_ROWS_IN_SINGLE_SQL_STATEMENT) {
            logger.info(String.format(
                    "Thread '%s' will not sleep because it updated near maximum row count.", threadName
            ));
        } else {
            sleep(timeSleepRandom.nextInt((int) (rescanTimeMillis / 5)));
        }
    }

    private long getRescanTimeMillis() {
        if (Inmemo.isDebug()) {
            return RESCAN_TIME_MILLIS; // * 20;
        } else {
            return RESCAN_TIME_MILLIS;
        }
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

        if (lastIndicatorValue == null && table.isUseJournal()) {
            //noinspection unchecked
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
            rows = jacuzzi.findRowRoll(
                    String.format(
                            "SELECT * FROM %s %s ORDER BY %s, %s LIMIT %d",
                            typeOracle.getTableName(),
                            forceIndexClause,
                            table.getIndicatorField(),
                            typeOracle.getIdColumn(),
                            MAX_ROWS_IN_SINGLE_SQL_STATEMENT
                    )
            );
        } else {
            rows = jacuzzi.findRowRoll(
                    String.format(
                            "SELECT * FROM %s %s WHERE %s >= ? ORDER BY %s, %s LIMIT %d",
                            typeOracle.getTableName(),
                            forceIndexClause,
                            table.getIndicatorField(),
                            table.getIndicatorField(),
                            typeOracle.getIdColumn(),
                            MAX_ROWS_IN_SINGLE_SQL_STATEMENT
                    ), indicatorLastValue
            );
        }

        long queryTimeMillis = System.currentTimeMillis() - startTimeMillis;
        if (queryTimeMillis * 10 > getRescanTimeMillis()) {
            logger.warn(String.format(
                    "Rescanning query for entity `%s` took too long time %d ms.",
                    table.getClazz().getName(), queryTimeMillis
            ));
        }
        return rows;
    }

    @SuppressWarnings("WeakerAccess")
    public static void setSpecificDataSource(Class<?> clazz, DataSource dataSource) {
        String clazzName = clazz.getName();
        logger.info("Setting specific data source for [clazz=" + clazzName + "].");
        dataSourceByClazzName.put(clazzName, dataSource);
    }

    private class TableUpdaterRunnable implements Runnable {
        @Override
        public void run() {
            @SuppressWarnings("UnsecureRandomNumberGeneration") Random sleepRandom = new Random();

            while (running) {
                try {
                    update(sleepRandom);
                } catch (Exception e) {
                    logger.error("Unexpected " + e.getClass().getName() + " exception in TableUpdaterRunnable of "
                            + threadName
                            + ": " + e, e);
                }
            }

            logger.warn("Inmemo update thread for " + table.getClazz().getName() + " finished");
        }
    }
}
