package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.NodePathContext;

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
    public String asLiteral(NodePathContext context) {
        return this.literal;
    }
}
