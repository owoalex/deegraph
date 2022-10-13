package org.deegraph.conditions;

import org.deegraph.database.*;
import org.deegraph.formats.ValueTypes;

import java.text.ParseException;

import static org.deegraph.formats.TypeCoercionUtilities.*;

public class ImplicitEqualityCondition extends CoerciveComparitorCondition {

    public ImplicitEqualityCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase, c1, c2);
    }

    @Override
    public String toString() {
        return "(" + c1.toString() + " = " + c2.toString() + ")";
    }

    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext nodePathContext) {
        try {
            String e1 = this.c1.asLiteral(securityContext,nodePathContext);
            String e2 = this.c2.asLiteral(securityContext,nodePathContext);
            e1 = literalToValue(e1, securityContext, nodePathContext);
            e2 = literalToValue(e2, securityContext, nodePathContext);

            if (e1 == null || e2 == null) {
                return (e1 == null && e2 == null);
            }

            ValueTypes type1 = detectType(e1);
            ValueTypes type2 = detectType(e2);

            try {
                if (type1.equals(ValueTypes.BOOL) || type2.equals(ValueTypes.BOOL)) {
                    return (coerceToBool(e1) == coerceToBool(e2));
                }
            } catch (NumberFormatException e) {

            }
            try {
                if (type1.equals(ValueTypes.NUMBER) || type2.equals(ValueTypes.NUMBER)) {
                    return (coerceToNumber(e1) == coerceToNumber(e2));
                }
            } catch (NumberFormatException e) {

            }
            return e1.equals(e2);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
