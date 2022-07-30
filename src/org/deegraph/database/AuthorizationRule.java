package org.deegraph.database;

import org.deegraph.conditions.Condition;

import java.util.UUID;

public class AuthorizationRule {
    boolean revoked = false;
    Condition condition;
    UUID uuid;
    AuthorizedAction[] authorizedActions;

    public AuthorizationRule(Condition condition, AuthorizedAction[] authorizedActions) {
        this.condition = condition;
        this.authorizedActions = authorizedActions;
        this.uuid = UUID.randomUUID();
    }

    public AuthorizationRule(Condition condition, AuthorizedAction[] authorizedActions, UUID uuid) {
        this.condition = condition;
        this.authorizedActions = authorizedActions;
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return uuid;
    }

    public AuthorizedAction[] getAuthorizableActions() {
        return authorizedActions;
    }

    public Authorization[] getAuthorizations(Node actor, Node object) {
        if (condition.eval(new NodePathContext(actor, object))) {
            Authorization[] authorizations = new Authorization[authorizedActions.length];
            for (int i = 0; i < authorizedActions.length; i++) {
                authorizations[i] = new Authorization(this.authorizedActions[i], this);
            }
            return authorizations;
        } else {
            return new Authorization[0];
        }
    }
}
