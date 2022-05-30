package com.indentationerror.dds;

public class NodePathContext {
    private Node actor;
    private Node object;

    public NodePathContext(Node actor, Node object) {
        this.actor = actor;
        this.object = object;
    }

    public Node getActor() {
        return this.actor;
    }

    public Node getObject() {
        return object;
    }
}
