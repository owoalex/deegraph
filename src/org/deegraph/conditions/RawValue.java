package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.NodePathContext;
import org.deegraph.database.SecurityContext;

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
