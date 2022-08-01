package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.NodePathContext;
import org.deegraph.database.SecurityContext;

public class LogicalNotCondition extends LogicalCondition {
    private Condition condition;

    public LogicalNotCondition(GraphDatabase graphDatabase, Condition condition) {
        super(graphDatabase);
        this.condition = condition;
    }

    @Override
    public String toString() {
        return "NOT " + condition.toString();
    }

    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext context) {
        return !condition.eval(securityContext, context);
    }
}
