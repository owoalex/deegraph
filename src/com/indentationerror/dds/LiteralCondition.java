package com.indentationerror.dds;

import java.util.Locale;

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
