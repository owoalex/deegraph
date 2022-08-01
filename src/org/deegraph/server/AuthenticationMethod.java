package org.deegraph.server;

import org.deegraph.database.AbsoluteNodePath;

public abstract class AuthenticationMethod {
    protected AbsoluteNodePath[] validForNodes;

    public boolean validate(Object object) {
        return false;
    }
}
