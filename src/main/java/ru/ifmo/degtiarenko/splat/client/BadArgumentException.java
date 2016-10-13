package ru.ifmo.degtiarenko.splat.client;

/**
 * Signals that a format of an method(s) argument is unacceptable.
 */
public class BadArgumentException extends Exception {
    public BadArgumentException(String badArgumentMessage) {
        super(badArgumentMessage);
    }

}
