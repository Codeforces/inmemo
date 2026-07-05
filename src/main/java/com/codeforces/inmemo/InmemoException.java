package com.codeforces.inmemo;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
@SuppressWarnings("WeakerAccess")
public class InmemoException extends RuntimeException {
    public InmemoException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public InmemoException(String message) {
        super(message);
    }
}
