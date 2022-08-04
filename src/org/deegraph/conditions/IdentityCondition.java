package org.deegraph.conditions;

import org.deegraph.database.*;

import java.text.ParseException;

public class IdentityCondition extends Condition {
    private Condition c1;
    private Condition c2;

    public IdentityCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase);
        this.c1 = c1;
        this.c2 = c2;
    }

    @Override
    public String toString() {
        return "(" + c1.toString() + " === " + c2.toString() + ")";
    }

    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext nodePathContext) {
        String e1 = this.c1.asLiteral(securityContext,nodePathContext);
        String e2 = this.c2.asLiteral(securityContext,nodePathContext);

        Node[] e1Nodes = new RelativeNodePath(e1).getMatchingNodes(securityContext, nodePathContext, securityContext.getDatabase().getAllNodesUnsafe());

        Node[] matchingNodes = new RelativeNodePath(e2).getMatchingNodes(securityContext, nodePathContext, e1Nodes);

        return (matchingNodes.length > 0); // Make sure there is an exact match
    }
}
