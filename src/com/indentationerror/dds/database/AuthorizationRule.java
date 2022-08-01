package com.indentationerror.dds.database;

import com.indentationerror.dds.conditions.Condition;

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

    public Authorization[] getAuthorizations(GraphDatabase graphDatabase, Node actor, Node object) {
        boolean checkPassed = (graphDatabase.getInstanceNode().equals(actor)); // Bypass for the instance node for performance - this node has all perms!
        checkPassed = checkPassed | (condition.eval(new SecurityContext(graphDatabase, graphDatabase.getInstanceNode()), new NodePathContext(actor, object))); // All grant conditions are evaluated as the instance node user
        if (checkPassed) {
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
