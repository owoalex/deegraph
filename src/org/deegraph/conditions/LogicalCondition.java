package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;

public abstract class LogicalCondition extends Condition {
    protected LogicalCondition(GraphDatabase graphDatabase) {
        super(graphDatabase);
    }
}
