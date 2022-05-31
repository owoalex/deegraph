package com.indentationerror.dds;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class Node {
    private UUID localId;
    private WUUID globalId;
    private Date cTime;
    private Date oCTime;
    private Node cNode;
    private WUUID oCNode;
    private String data;
    private String schema;
    private HashMap<String, Node> properties;

    private HashMap<String, ArrayList<Node>> references;
    Node(UUID localId, WUUID globalId, Node cNode, WUUID oCNode, String data, String schema) {
        this.cTime = new Date();
        this.oCTime = new Date();
        this.oCNode = oCNode;
        this.data = data;
        this.schema = schema;
        this.properties = new HashMap<>();
        this.references = new HashMap<>(); // References is like the inverse lookup of properties - the database has to work hard to keep these consistent!
        this.localId = localId;
        this.globalId = globalId;
        this.cNode = cNode;
    }

    void makeSelfReferential() {
        this.oCNode = this.globalId;
        this.cNode = this;
    }

    public UUID getId() {
        return localId;
    }

    public WUUID getGlobalId() {
        return globalId;
    }

    public Date getCTime() {
        return this.cTime;
    }

    public Date getOCTime() {
        return this.oCTime;
    }

    public Node getCNode() {
        return this.cNode;
    }

    void setCNode(Node cNode) {
        this.cNode = cNode;
    }

    public WUUID getOCNodeId() {
        return this.oCNode;
    }

    public String getData() {
        return this.data;
    }

    public String getSchema() {
        return this.schema;
    }

    public HashMap<String, Node> getProperties() {
        return this.properties;
    }

    public void removeProperty(String name) {
        if (this.properties.containsKey(name)) {
            this.properties.get(name).references.get(name).remove(this); // Make sure to remove the reverse lookup
            this.properties.remove(name); // Then remove the property
        }
    }
    public void addProperty(String name, Node node) throws DuplicatePropertyException {
        if (this.properties.containsKey(name)) {
            throw new DuplicatePropertyException();
        } else {
            this.properties.put(name, node);
            if (!node.references.containsKey(name)) {
                node.references.put(name, new ArrayList<>());
            }
            node.references.get(name).add(this); // Add reverse lookup
        }
    }

    public Node getProperty(String name) {
        if (this.properties.containsKey(name)) {
            return this.properties.get(name);
        } else {
            return null;
        }
    }

    public Node[] getReferrers(String name) {
        if (this.references.containsKey(name)) {
            Node[] nodes = this.references.get(name).toArray(new Node[0]);
            return (nodes.length == 0) ? null : nodes; // Don't return an empty array, just return null
        } else {
            return null;
        }
    }
}
