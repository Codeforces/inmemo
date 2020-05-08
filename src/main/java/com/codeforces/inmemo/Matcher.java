package com.codeforces.inmemo;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public interface Matcher<T extends HasId> {
    boolean match(T tableItem);
}
