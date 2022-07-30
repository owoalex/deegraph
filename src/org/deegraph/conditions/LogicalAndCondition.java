package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.NodePathContext;

public class LogicalAndCondition extends LogicalCondition {
    private Condition condition1;
    private Condition condition2;

    public LogicalAndCondition(GraphDatabase graphDatabase, Condition condition1, Condition condition2) {
        super(graphDatabase);
        this.condition1 = condition1;
        this.condition2 = condition2;
    }

    @Override
    public String toString() {
        return "(" + condition1.toString() + " && " + condition2.toString() + ")";
    }

    @Override
    public boolean eval(NodePathContext context) {
        return condition1.eval(context) && condition2.eval(context);
    }
}
