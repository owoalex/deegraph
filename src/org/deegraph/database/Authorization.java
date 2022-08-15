package org.deegraph.database;

public class Authorization {
    private AuthorizedAction[] actions;
    private AuthorizationRule basis;
    private Node validFrom;
    private Node actor;
    private GraphDatabase gdb;
    private RelativeNodePath[] validFor;

    Authorization(RelativeNodePath[] validFor, Node validFrom, Node actor, GraphDatabase gdb, AuthorizedAction[] actions, AuthorizationRule basis) {
        this.actor = actor;
        this.gdb = gdb;
        this.validFrom = validFrom;
        this.validFor = validFor;
        this.actions = actions;
        this.basis = basis;
    }

    public AuthorizationRule getBasis() {
        return this.basis;
    }

    public RelativeNodePath[] getValidPaths() {
        return this.validFor;
    }

    public Node getValidFrom() {
        return this.validFrom;
    }

    public boolean isValidForNode(Node node) {
        if (node == null) {
            return false;
        }
        if (this.validFor == null) {
            if (this.validFrom != null) {
                if (node.equals(this.validFrom)) {
                    //System.out.println(this.validFrom.getId() + " == " + node.getId() + " (Explicit validFrom, implicit validFor) ✔");
                    return true;
                }
            }
        }
        for (RelativeNodePath rnp: this.validFor) {
            Node[] validMatches = rnp.getMatchingNodes(new SecurityContext(this.gdb, this.gdb.getInstanceNode()), new NodePathContext(this.actor, this.validFrom), new Node[] {node});
            if (validMatches.length > 0) {
                //System.out.println(rnp + " == " + node.getId() + " ✔️");
                return true;
            }
        }
       // System.out.println("" + node.getId() + " does not match validFor constraint ❌️");
        return false;
    }

    public AuthorizedAction[] getActions() {
        return this.actions;
    }
}
