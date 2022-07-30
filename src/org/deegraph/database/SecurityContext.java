package org.deegraph.database;

public class SecurityContext {
    Node actor;
    GraphDatabase database;

    public SecurityContext(GraphDatabase database, Node actor) {
        this.actor = actor;
        this.database = database;
    }

    public Node getActor() {
        return actor;
    }

    public GraphDatabase getDatabase() {
        return database;
    }
}
