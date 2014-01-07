package com.codeforces.inmemo;

import org.jacuzzi.core.Row;

import javax.annotation.Nonnull;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class RowListener {
    private final String name;
    private final Listener listener;

    public RowListener(@Nonnull String name, @Nonnull Listener listener) {
        this.name = name;
        this.listener = listener;
    }

    public void insertOrUpdate(final Row row) {
        listener.onInsertOrUpdate(row);
    }

    public String getName() {
        return name;
    }

    public static interface Listener {
        void onInsertOrUpdate(Row row);
    }
}
