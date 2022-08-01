package org.deegraph.database;

public class NodePathContext {
    private Node actor;
    private Node requestingNode;
    private Node object;

    public NodePathContext(Node actor, Node object) {
        this.actor = actor;
        this.object = object;
    }

    public NodePathContext(Node actor, Node object, Node requestingNode) {
        this.actor = actor;
        this.object = object;
        this.requestingNode = requestingNode;
    }

    public Node getRequestingNode() {
        return this.requestingNode;
    }
    public Node getActor() {
        return this.actor;
    }

    public Node getObject() {
        return object;
    }
}
