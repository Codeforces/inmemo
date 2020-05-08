package com.codeforces.inmemo;

import javax.annotation.Nonnull;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class ItemListener<T extends HasId> {
    private final String name;
    private final Listener<T> listener;

    public ItemListener(@Nonnull String name, @Nonnull Listener<T> listener) {
        this.name = name;
        this.listener = listener;
    }

    public void insertOrUpdate(@Nonnull T item) {
        listener.onInsertOrUpdate(item);
    }

    public String getName() {
        return name;
    }

    public interface Listener<T extends HasId> {
        void onInsertOrUpdate(T item);
    }
}
