package com.codeforces.inmemo;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class InmemoException extends RuntimeException {
    public InmemoException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public InmemoException(final String message) {
        super(message);
    }
}
