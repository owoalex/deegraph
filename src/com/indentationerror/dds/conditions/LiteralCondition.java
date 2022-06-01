package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.DatabaseInstance;
import com.indentationerror.dds.database.NodePathContext;

public class LiteralCondition extends Condition {
    private String literal;

    public LiteralCondition(DatabaseInstance databaseInstance, String literal) {
        super(databaseInstance);
        this.literal = literal;
    }

    @Override
    public String asLiteral(NodePathContext context) {
        return this.literal;
    }
}
