package com.codeforces.inmemo;

import javax.annotation.Nonnull;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class ItemListener<T extends HasId> {
    private final String name;
    private final Listener<T> listener;

    public ItemListener(@Nonnull final String name, @Nonnull final Listener<T> listener) {
        this.name = name;
        this.listener = listener;
    }

    public void insertOrUpdate(@Nonnull final T item) {
        listener.onInsertOrUpdate(item);
    }

    public String getName() {
        return name;
    }

    public static interface Listener<T extends HasId> {
        void onInsertOrUpdate(T item);
    }
}
