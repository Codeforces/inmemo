package com.codeforces.inmemo;

import org.apache.commons.lang.ObjectUtils;
import org.apache.log4j.Logger;
import org.jacuzzi.core.Jacuzzi;
import org.jacuzzi.core.Row;
import org.jacuzzi.core.TypeOracle;

import javax.sql.DataSource;
import java.util.*;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
class TableUpdater<T extends HasId> {
    private static final Logger logger = Logger.getLogger(TableUpdater.class);
    private static final int MAX_ROWS_IN_SINGLE_SQL_STATEMENT = 200_000;

    private static DataSource dataSource;
    private static final Collection<TableUpdater<? extends HasId>> instances = new ArrayList<>();

    private final Table table;
    private final Thread thread;
    private final String threadName;
    private volatile boolean running;

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;

    private Object lastIndicatorValue;
    private final long startTimeMillis;

    /**
     * Delay after meaningless update try.
     */
    private static final long rescanTimeMillis = 500;

    private final Collection<Long> lastUpdatedEntityIds = new HashSet<>();

    TableUpdater(Table<T> table, Object initialIndicatorValue) {
        if (dataSource == null) {
            logger.error("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
            throw new InmemoException("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
        }

        this.table = table;
        this.lastIndicatorValue = initialIndicatorValue;

        jacuzzi = Jacuzzi.getJacuzzi(dataSource);
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

    void insertOrUpdateById(Long id) {
        List<Row> rows = jacuzzi.findRows(String.format("SELECT * FROM %s WHERE %s = %s",
                typeOracle.getTableName(), typeOracle.getIdColumn(), id.toString()
        ));

        if (rows == null || rows.isEmpty()) {
            return;
        }

        if (rows.size() == 1) {
            Row row = rows.get(0);
            T entity = typeOracle.convertFromRow(row);

            table.insertOrUpdate(entity);
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

        List<Row> rows = jacuzzi.findRows(String.format("SELECT * FROM %s WHERE %s ORDER BY %s",
                typeOracle.getTableName(), formattedFields, typeOracle.getIdColumn()), fieldValues);

        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        logger.warn("Emergency case: found " + rows.size() + " items of class " + table.getClazz().getName() + " [fields=" + formattedFields + "].");

        List<T> result = new ArrayList<>(rows.size());
        for (Row row : rows) {
            T entity = typeOracle.convertFromRow(row);
            logger.warn("Emergency found: " + table.getClazz().getName() + " id=" + entity.getId() + " [fields=" + formattedFields + "].");

            result.add(entity);

            table.insertOrUpdate(entity);
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
            if (field == null || !(field instanceof String) || ((String) field).isEmpty()) {
                throw new IllegalArgumentException("EmergencyQueryFields array must contain non-empty strings on even positions. Found: "
                        + Arrays.toString(fields) + '.');
            }
        }
    }

    private void update() {
        long startTimeMillis = System.currentTimeMillis();

        List<Row> rows = getRecentlyChangedRows(lastIndicatorValue);

        if (rows.size() >= 10) {
            logger.info(String.format("Thread '%s' has found %s rows to update in %d ms [lastIndicatorValue=" + lastIndicatorValue + "].", threadName,
                    rows.size(), System.currentTimeMillis() - startTimeMillis));
            if (rows.size() <= 100) {
                StringBuilder ids = new StringBuilder();
                for (Row row : rows) {
                    if (ids.length() > 0) {
                        ids.append(',');
                    }
                    ids.append(row.get(typeOracle.getIdColumn()));
                }
                logger.info("Updated entries have id=" + ids + '.');
            }
        }

        int updatedCount = 0;
        Object previousIndicatorLastValue = lastIndicatorValue;

        boolean hasInsertOrUpdateByRow = table.hasInsertOrUpdateByRow();

        for (Row row : rows) {
            long id = getRowId(row);
            if (ObjectUtils.equals(row.get(table.getIndicatorField()), previousIndicatorLastValue)
                    && lastUpdatedEntityIds.contains(id)) {
                // logger.info("Thread " + threadName + " skipped id=" + id + ".");
                continue;
            }

            // Insert or update entity.
            T entity = typeOracle.convertFromRow(row);
            table.insertOrUpdate(entity);

            // Insert or update row.
            if (hasInsertOrUpdateByRow) {
                table.insertOrUpdate(row);
            }

            // logger.log(Level.INFO, "Updated entity " + entity + '.');
            lastIndicatorValue = row.get(table.getIndicatorField());
            ++updatedCount;
        }

        if (updatedCount >= 10) {
            logger.info(String.format("Thread '%s' has updated %d items in %d ms [lastIndicatorValue=" + lastIndicatorValue + "].",
                    threadName, updatedCount, System.currentTimeMillis() - startTimeMillis));
        }

        if (rows.size() <= lastUpdatedEntityIds.size()
                && rows.size() < MAX_ROWS_IN_SINGLE_SQL_STATEMENT / 2
                && !table.isPreloaded()) {
            logger.info("Inmemo preloaded " + ReflectionUtil.getTableClassName(table.getClazz())
                    + " [items=" + table.size() + "] in " + (System.currentTimeMillis() - TableUpdater.this.startTimeMillis) + " ms.");
            table.setPreloaded(true);
        }

        lastUpdatedEntityIds.clear();
        for (Row row : rows) {
            if (ObjectUtils.equals(row.get(table.getIndicatorField()), lastIndicatorValue)) {
                lastUpdatedEntityIds.add(getRowId(row));
            }
        }

        sleepBetweenRescans(updatedCount);
    }

    private static long getRowId(Row row) {
        Long id = (Long) row.get("id");
        if (id == null) {
            id = (Long) row.get("ID");
        }
        return id;
    }

    private void sleepBetweenRescans(int updatedCount) {
        if (updatedCount == 0) {
            sleep(rescanTimeMillis);
        } else if ((updatedCount << 1) > MAX_ROWS_IN_SINGLE_SQL_STATEMENT) {
            logger.info(String.format(
                    "Thread '%s' will not sleep because it updated near maximum row count.", threadName
            ));
        } else {
            sleep(rescanTimeMillis / 10);
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

    private List<Row> getRecentlyChangedRows(Object indicatorLastValue) {
        List<Row> rows;
        long startTimeMillis = System.currentTimeMillis();
        String forceIndexClause = table.getDatabaseIndex() == null ? "" : ("FORCE INDEX (" + table.getDatabaseIndex() + ')');

        if (indicatorLastValue == null) {
            rows = jacuzzi.findRows(
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
            rows = jacuzzi.findRows(
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
        if (queryTimeMillis * 10 > rescanTimeMillis) {
            logger.warn(String.format(
                    "Rescanning query for entity `%s` took too long time %d ms.",
                    table.getClazz().getName(), queryTimeMillis
            ));
        }

        int rowCount = rows.size();
        if (rowCount == MAX_ROWS_IN_SINGLE_SQL_STATEMENT) {
            logger.warn(String.format(
                    "Suspicious row count while rescanning `%s` [rowCount=%d, queryTime=%d ms].",
                    table.getClazz().getName(),
                    MAX_ROWS_IN_SINGLE_SQL_STATEMENT,
                    queryTimeMillis
            ));
        }

        return rows;
    }

    private class TableUpdaterRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    update();
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
