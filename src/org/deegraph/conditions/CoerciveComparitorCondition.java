package org.deegraph.conditions;

import org.deegraph.database.*;

import java.text.ParseException;
import java.util.Arrays;

import static org.deegraph.database.NodePath.metaProp;

public class CoerciveComparitorCondition extends Condition {
    protected Condition c1;
    protected Condition c2;

    public CoerciveComparitorCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase);
        this.c1 = c1;
        this.c2 = c2;
    }

    protected String literalToValue(String literal, SecurityContext securityContext, NodePathContext nodePathContext) throws ParseException {
        Node requestingNode = nodePathContext.getActor();
        if (nodePathContext.getRequestingNode() != null) {
            requestingNode = nodePathContext.getRequestingNode();
        }

        if (literal.startsWith("\"")) { // Decide whether to treat as literal or not
            return literal.substring(1, literal.length() - 1);
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
            Node node = new RelativeNodePath(literal).toAbsolute(nodePathContext).getNodeFrom(this.graphDatabase, securityContext);
            if (node != null) {
                return metaProp(this.graphDatabase, node, prop, requestingNode);
            }
        }
        return null;
    }
}
