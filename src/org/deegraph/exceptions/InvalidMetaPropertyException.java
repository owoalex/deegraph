package org.deegraph.exceptions;

public class InvalidMetaPropertyException extends Exception {
    public InvalidMetaPropertyException(String property) {
        super("Invalid meta property " + property);
    }
}
