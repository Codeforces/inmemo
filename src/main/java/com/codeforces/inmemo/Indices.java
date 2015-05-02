package com.codeforces.inmemo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class Indices<T extends HasId> {
    private final List<Index<T, ?>> indices = new ArrayList<>();
    private final List<RowListener> rowListeners = new ArrayList<>();
    private final List<ItemListener<T>> itemListeners = new ArrayList<>();

    private void add(Index<T, ?> index) {
        indices.add(index);
    }

    private void add(RowListener rowListener) {
        rowListeners.add(rowListener);
    }

    private void add(ItemListener<T> itemListener) {
        itemListeners.add(itemListener);
    }

    List<Index<T, ?>> getIndices() {
        return Collections.unmodifiableList(indices);
    }

    List<RowListener> getRowListeners() {
        return Collections.unmodifiableList(rowListeners);
    }

    List<ItemListener<T>> getItemListeners() {
        return Collections.unmodifiableList(itemListeners);
    }

    public static class Builder<T extends HasId> {
        private final Collection<Index<T, ?>> indices = new ArrayList<>();
        private final Collection<RowListener> rowListeners = new ArrayList<>();
        private final Collection<ItemListener<T>> itemListeners = new ArrayList<>();

        public <V> void add(Index<T, V> index) {
            indices.add(index);
        }

        public <V> void add(RowListener rowListener) {
            rowListeners.add(rowListener);
        }

        public <V> void add(ItemListener<T> itemListener) {
            itemListeners.add(itemListener);
        }

        public Indices<T> build() {
            Indices<T> result = new Indices<>();

            for (Index<T, ?> index : indices) {
                result.add(index);
            }

            for (RowListener rowListener : rowListeners) {
                result.add(rowListener);
            }

            for (ItemListener<T> itemListener : itemListeners) {
                result.add(itemListener);
            }

            return result;
        }
    }
}
