package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.NodePathContext;
import org.deegraph.database.SecurityContext;

import java.text.ParseException;

import static org.deegraph.formats.TypeCoercionUtilities.coerceToNumber;

public class LessThanCondition extends CoerciveComparitorCondition {

    public LessThanCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase, c1, c2);
    }

    @Override
    public String toString() {
        return "(" + c1.toString() + " < " + c2.toString() + ")";
    }

    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext nodePathContext) {
        try {
            String e1 = this.c1.asLiteral(securityContext,nodePathContext);
            String e2 = this.c2.asLiteral(securityContext,nodePathContext);
            e1 = literalToValue(e1, securityContext, nodePathContext);
            e2 = literalToValue(e2, securityContext, nodePathContext);

            return (coerceToNumber(e1) < coerceToNumber(e2));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
