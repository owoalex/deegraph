package org.deegraph.exceptions;

import org.deegraph.database.Node;

public class DuplicateNodeStoreException extends Exception {
    public DuplicateNodeStoreException(Node node) {
        super(node.getId().toString());
    }
}
