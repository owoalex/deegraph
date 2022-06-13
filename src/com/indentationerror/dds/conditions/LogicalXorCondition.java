package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.database.GraphDatabaseBacking;
import com.indentationerror.dds.database.NodePathContext;

public class LogicalXorCondition extends LogicalCondition {
    private Condition condition1;
    private Condition condition2;

    public LogicalXorCondition(GraphDatabase graphDatabase, Condition condition1, Condition condition2) {
        super(graphDatabase);
        this.condition1 = condition1;
        this.condition2 = condition2;
    }

    @Override
    public boolean eval(NodePathContext context) {
        return condition1.eval(context) ^ condition2.eval(context);
    }
}
