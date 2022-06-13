package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.database.NodePathContext;

public class RawValue extends Condition {
    private String literal;

    public RawValue(GraphDatabase graphDatabase, String literal) {
        super(graphDatabase);
        this.literal = literal;
    }

    @Override
    public String asLiteral(NodePathContext context) {
        return this.literal;
    }
}
