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

    private final Table<T> table;
    private final Thread thread;
    private final String threadName;
    private volatile boolean running;

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;

    private Object indicatorLastValue;

    /** Delay after meaningless update try. */
    private static final long rescanTimeMillis = 1000;

    private final Collection<Long> lastUpdatedEntityIds = new HashSet<>();

    TableUpdater(final Table<T> table) {
        if (dataSource == null) {
            logger.error("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
            throw new InmemoException("It should be called static Inmemo#setDataSource() before any instance of TableUpdater.");
        }

        this.table = table;

        jacuzzi = Jacuzzi.getJacuzzi(dataSource);
        typeOracle = TypeOracle.getTypeOracle(table.getClazz());

        threadName = "InmemoUpdater#" + table.getClazz();
        thread = new Thread(new TableUpdaterRunnable(), threadName);
        thread.setDaemon(true);

        logger.info("Started Inmemo table updater thread '" + threadName + "'.");

        //noinspection ThisEscapedInObjectConstruction
        instances.add(this);
    }

    @SuppressWarnings("UnusedDeclaration")
    static void setDataSource(final DataSource dataSource) {
        TableUpdater.dataSource = dataSource;
    }

    @SuppressWarnings("UnusedDeclaration")
    static void stop() {
        for (final TableUpdater<? extends HasId> instance : instances) {
            instance.running = false;
        }
    }

    void start() {
        running = true;
        thread.start();
    }

    private void update() {
        final List<Row> rows = getRecentlyChangedRows(indicatorLastValue);

        if (rows.size() >= 10) {
            logger.info(String.format("Thread '%s' has found %s rows to update.", threadName, rows.size()));
        }

        int updatedCount = 0;
        final Object previousIndicatorLastValue = indicatorLastValue;

        for (final Row row : rows) {
            final T entity = typeOracle.convertFromRow(row);
            if (ObjectUtils.equals(row.get(table.getIndicatorField()), previousIndicatorLastValue)
                    && lastUpdatedEntityIds.contains(entity.getId())) {
                continue;
            }

            table.insertOrUpdate(entity);
            // logger.log(Level.INFO, "Updated entity " + entity + '.');
            indicatorLastValue = row.get(table.getIndicatorField());
            ++updatedCount;
        }

        if (updatedCount > 0) {
            logger.info(String.format("Thread '%s' has updated %d items.", threadName, updatedCount));
        }

        lastUpdatedEntityIds.clear();

        for (final Row row : rows) {
            if (ObjectUtils.equals(row.get(table.getIndicatorField()), indicatorLastValue)) {
                lastUpdatedEntityIds.add((Long) row.get("id"));
            }
        }

        if (rows.size() < MAX_ROWS_IN_SINGLE_SQL_STATEMENT) {
            table.setPreloaded(true);
        }

        sleepBetweenRescans(updatedCount);
    }

    private void sleepBetweenRescans(final int updatedCount) {
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

    private void sleep(final long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            logger.error("Thread '" + threadName + "' has been stopped because of InterruptedException.", e);
            running = false;
        }
    }

    private List<Row> getRecentlyChangedRows(final Object indicatorLastValue) {
        final List<Row> rows;
        final long startTimeMillis = System.currentTimeMillis();

        if (indicatorLastValue == null) {
            rows = jacuzzi.findRows(
                    String.format(
                            "SELECT * FROM %s ORDER BY %s, %s LIMIT %d",
                            typeOracle.getTableName(),
                            table.getIndicatorField(),
                            typeOracle.getIdColumn(),
                            MAX_ROWS_IN_SINGLE_SQL_STATEMENT
                    )
            );
        } else {
            rows = jacuzzi.findRows(
                    String.format(
                            "SELECT * FROM %s WHERE %s >= ? ORDER BY %s, %s LIMIT %d",
                            typeOracle.getTableName(),
                            table.getIndicatorField(),
                            table.getIndicatorField(),
                            typeOracle.getIdColumn(),
                            MAX_ROWS_IN_SINGLE_SQL_STATEMENT
                    ), indicatorLastValue
            );
        }

        final long queryTimeMillis = System.currentTimeMillis() - startTimeMillis;
        if (queryTimeMillis * 10 > rescanTimeMillis) {
            logger.warn(String.format(
                    "Rescanning query for entity `%s` took too long time %d ms.",
                    table.getClazz().getName(), queryTimeMillis
            ));
        }

        final int rowCount = rows.size();
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
        }

    }
}
