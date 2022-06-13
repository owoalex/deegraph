package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;

public abstract class LogicalCondition extends Condition {
    protected LogicalCondition(GraphDatabase graphDatabase) {
        super(graphDatabase);
    }
}
