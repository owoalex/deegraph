package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.database.NodePathContext;
import com.indentationerror.dds.database.SecurityContext;

public class LogicalXorCondition extends LogicalCondition {
    private Condition condition1;
    private Condition condition2;

    public LogicalXorCondition(GraphDatabase graphDatabase, Condition condition1, Condition condition2) {
        super(graphDatabase);
        this.condition1 = condition1;
        this.condition2 = condition2;
    }

    @Override
    public String toString() {
        return "(" + condition1.toString() + " XOR " + condition2.toString() + ")";
    }
    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext context) {
        return condition1.eval(securityContext, context) ^ condition2.eval(securityContext, context);
    }
}
