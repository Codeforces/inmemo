package com.codeforces.inmemo;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public interface IndexGetter<T extends HasId, V> {
    V get(T tableItem);
}
