package com.indentationerror.dds.server;

import com.indentationerror.dds.database.AbsoluteNodePath;

public abstract class AuthenticationMethod {
    protected AbsoluteNodePath[] validForNodes;

    public boolean validate(Object object) {
        return false;
    }
}
