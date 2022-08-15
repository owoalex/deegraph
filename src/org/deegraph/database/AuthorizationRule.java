package org.deegraph.database;

import org.deegraph.conditions.Condition;

import java.util.UUID;

public class AuthorizationRule {
    private boolean revoked = false;
    private Condition condition;
    private RelativeNodePath[] validFor;
    private UUID uuid;
    private AuthorizedAction[] authorizedActions;

    private boolean delegatable;

    public AuthorizationRule(RelativeNodePath[] validFor, Condition condition, AuthorizedAction[] authorizedActions, boolean delegatable) {
        if (validFor != null) {
            if (validFor.length != 0) {
                this.validFor = validFor;
            }
        }
        this.condition = condition;
        this.authorizedActions = authorizedActions;
        this.uuid = UUID.randomUUID();
        this.delegatable = delegatable;
    }

    public AuthorizationRule(RelativeNodePath[] validFor, Condition condition, AuthorizedAction[] authorizedActions, boolean delegatable, UUID uuid) {
        if (validFor != null) {
            if (validFor.length != 0) {
                this.validFor = validFor;
            }
        }
        this.condition = condition;
        this.authorizedActions = authorizedActions;
        this.uuid = uuid;
        this.delegatable = delegatable;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Condition getCondition() {
        return condition;
    }

    public AuthorizedAction[] getAuthorizableActions() {
        return authorizedActions;
    }

    public boolean isDelegatable() {
        return delegatable;
    }

    public Authorization getAuthorization(GraphDatabase graphDatabase, Node actor, Node object) {
        boolean checkPassed = (graphDatabase.getInstanceNode().equals(actor)); // Bypass for the instance node for performance - this node has all perms!
        if (condition == null) {
            checkPassed = true; // If there's not a condition - then this is unconditional!
        } else {
            checkPassed = checkPassed | (condition.eval(new SecurityContext(graphDatabase, graphDatabase.getInstanceNode()), new NodePathContext(actor, object))); // All grant conditions are evaluated as the instance node user
        }
        if (checkPassed) {
            if (graphDatabase.getDebugSetting()) {
                if (!graphDatabase.getInstanceNode().equals(actor)) {
                    System.out.println(this.condition + " ✔️");
                    System.out.print("    Actions: [ ");
                    for (AuthorizedAction authorizedAction: this.authorizedActions) {
                        System.out.print(authorizedAction + " ");
                    }
                    System.out.println("]");
                    if (this.validFor != null) {
                        System.out.println("    Valid for:");
                        for (RelativeNodePath relativeNodePath : this.validFor) {
                            System.out.println("        " + relativeNodePath);
                        }
                    }
                }

            }
            return new Authorization(this.validFor, object, actor, graphDatabase, this.authorizedActions, this);
        } else {
            if (graphDatabase.getDebugSetting()) {
                System.out.println(this.condition + " ❌");
            }
            return null;
        }
    }
}
