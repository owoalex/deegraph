package com.indentationerror.dds.conditions;

import com.indentationerror.dds.database.GraphDatabase;
import com.indentationerror.dds.database.NodePathContext;
import com.indentationerror.dds.database.SecurityContext;

public class RawValue extends Condition {
    private String literal;

    public RawValue(GraphDatabase graphDatabase, String literal) {
        super(graphDatabase);
        this.literal = literal;
    }

    @Override
    public String toString() {
        return this.literal;
    }

    @Override
    public String asLiteral(SecurityContext securityContext, NodePathContext context) {
        return this.literal;
    }
}
