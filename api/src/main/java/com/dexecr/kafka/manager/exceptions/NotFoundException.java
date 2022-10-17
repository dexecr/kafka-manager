package com.dexecr.kafka.manager.exceptions;

public class NotFoundException extends RuntimeException {
    public NotFoundException(String s) {
        super(s);
    }

    public NotFoundException(String s, Object... p) {
        super(String.format(s, p));
    }
}
