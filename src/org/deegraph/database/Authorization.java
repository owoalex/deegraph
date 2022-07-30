package org.deegraph.database;

public class Authorization {
    private AuthorizedAction action;
    private AuthorizationRule basis;

    Authorization(AuthorizedAction action, AuthorizationRule basis) {
        this.action = action;
        this.basis = basis;
    }

    public AuthorizationRule getBasis() {
        return basis;
    }

    public AuthorizedAction getAction() {
        return action;
    }
}
