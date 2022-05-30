package com.indentationerror.dds;

import java.util.ArrayList;

public class AuthorizationRule {
    boolean revoked = false;
    Condition condition;
    AuthorizedAction[] authorizedActions;

    AuthorizationRule(Condition condition, AuthorizedAction[] authorizedActions) {
        this.condition = condition;
        this.authorizedActions = authorizedActions;
    }

    public Authorization[] getAuthorizations(Node actor, Node object) {
        Authorization[] authorizations = new Authorization[authorizedActions.length];
        for (int i = 0; i < authorizedActions.length; i++) {
            authorizations[i] = new Authorization(this.authorizedActions[i], this);
        }
        return authorizations;
    }
}
