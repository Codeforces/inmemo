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

    private void add(final Index<T, ?> index) {
        indices.add(index);
    }

    List<Index<T, ?>> getIndices() {
        return Collections.unmodifiableList(indices);
    }

    public static class Builder<T extends HasId> {
        private final Collection<Index<T, ?>> indices = new ArrayList<>();

        public <V> void add(final Index<T, V> index) {
            indices.add(index);
        }

        public Indices<T> build() {
            final Indices<T> result = new Indices<>();

            for (final Index<T, ?> index : indices) {
                result.add(index);
            }

            return result;
        }
    }
}
