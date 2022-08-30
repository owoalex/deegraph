package org.deegraph.conditions;

import org.deegraph.database.*;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;

public class EqualityCondition extends Condition {
    private Condition c1;
    private Condition c2;

    public EqualityCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase);
        this.c1 = c1;
        this.c2 = c2;
    }

    @Override
    public String toString() {
        return "(" + c1.toString() + " == " + c2.toString() + ")";
    }

    private byte[] literalToRawValue(String literal, SecurityContext securityContext, NodePathContext nodePathContext) throws ParseException {
        Node requestingNode = nodePathContext.getActor();
        if (nodePathContext.getRequestingNode() != null) {
            requestingNode = nodePathContext.getRequestingNode();
        }

        if (literal.startsWith("\"")) { // Decide whether to treat as literal or not
            return literal.substring(1, literal.length() - 1).getBytes(StandardCharsets.UTF_8);
        } else {
            boolean absolute = literal.startsWith("/");
            String[] components = literal.split("/");
            String prop = "@data";
            if (components[components.length - 1].startsWith("@")) {
                prop = components[components.length - 1];
                literal = String.join("/", Arrays.copyOfRange(components, 0, components.length - 1));
                if (!absolute && literal.length() == 0) {
                    literal = "."; // Special case, empty strings resulting from removing an @ property should be changed to a cd operator
                }
            }
            if (absolute) {
                literal = "/" + literal;
            }
            Node e1Node = new RelativeNodePath(literal).toAbsolute(nodePathContext).getNodeFrom(this.graphDatabase, securityContext);
            if (e1Node != null) {
                return metaPropRaw(e1Node, prop, nodePathContext, requestingNode);
            }
        }
        return new byte[0];
    }
    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext nodePathContext) {
        try {
            String e1 = this.c1.asLiteral(securityContext,nodePathContext);
            String e2 = this.c2.asLiteral(securityContext,nodePathContext);


            //System.out.println(e1 + " == " + e2);

            byte[] rawValue1 = literalToRawValue(e1, securityContext, nodePathContext);
            byte[] rawValue2 = literalToRawValue(e2, securityContext, nodePathContext);

            if (rawValue1 == null || rawValue2 == null) {
                return (rawValue1 == null && rawValue2 == null);
            }
            //System.out.println(new String(rawValue1) + " == " + new String(rawValue2) + " : " + (Arrays.equals(rawValue1, rawValue2) ? "TRUE" : "FALSE"));
            return Arrays.equals(rawValue1, rawValue2);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
