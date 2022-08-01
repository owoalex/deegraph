package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.database.NodePathContext;
import com.indentationerror.dds.database.SecurityContext;

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
