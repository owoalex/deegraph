package com.indentationerror.dds.exceptions;

import com.indentationerror.dds.database.Node;

public class DuplicateNodeStoreException extends Exception {
    public DuplicateNodeStoreException(Node node) {
        super();
    }
}
