package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.DatabaseInstance;

public abstract class LogicalCondition extends Condition {
    protected LogicalCondition(DatabaseInstance databaseInstance) {
        super(databaseInstance);
    }
}
