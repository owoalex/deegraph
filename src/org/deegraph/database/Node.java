package org.deegraph.database;

import org.deegraph.exceptions.DuplicatePropertyException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

public class Node {
    private UUID localId;
    private UUID originalId;
    private UUID originalInstanceId;
    private Date cTime;
    private Date oCTime;
    private Node cNode;
    private UUID oCNode;

    private String data;
    private String schema;
    private HashMap<String, Node> properties;

    private HashMap<String, ArrayList<Node>> references;

    private TrustBlock trustRoot;
    private GraphDatabase gdb;

    Node(GraphDatabase gdb, UUID localId, UUID originalId, UUID originalInstanceId, Node cNode, UUID oCNode, String data, String schema) {
        this.cTime = new Date();
        this.oCTime = new Date();
        this.oCNode = oCNode;
        this.data = data;
        this.schema = schema;
        this.properties = new HashMap<>();
        this.references = new HashMap<>(); // References is like the inverse lookup of properties - the database has to work hard to keep these consistent!
        this.localId = localId;
        this.originalId = originalId;
        this.originalInstanceId = originalInstanceId;
        this.cNode = cNode;
        this.gdb = gdb;
        this.trustRoot = TrustBlock.createRoot(this, gdb);
    }

    Node(GraphDatabase gdb, UUID localId, UUID originalId, UUID originalInstanceId, Node cNode, UUID oCNode, String data, String schema, Date cTime, Date oCTime, TrustBlock trustRoot) {
        this.cTime = cTime;
        this.oCTime = oCTime;
        this.oCNode = oCNode;
        this.data = data;
        this.schema = schema;
        this.properties = new HashMap<>();
        this.references = new HashMap<>(); // References is like the inverse lookup of properties - the database has to work hard to keep these consistent!
        this.localId = localId;
        this.originalId = originalId;
        this.originalInstanceId = originalInstanceId;
        this.cNode = cNode;
        this.gdb = gdb;
        this.trustRoot = trustRoot;
    }

    public TrustBlock getTrustRoot() {
        return trustRoot;
    }
    public byte[] getHash() {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        String data = this.getDataUnsafe();
        String schema = this.getSchema();
        byte[] dataBytes = (data == null) ? new byte[0] : data.getBytes(StandardCharsets.UTF_8);
        byte[] schemaBytes = (schema == null) ? new byte[0] : schema.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(new byte[32 + 8 + 32 + dataBytes.length + schemaBytes.length]);
        bb.putLong(this.getOCNodeId().getMostSignificantBits());
        bb.putLong(this.getOCNodeId().getLeastSignificantBits());
        bb.putLong(this.getOriginalInstanceId().getMostSignificantBits());
        bb.putLong(this.getOriginalInstanceId().getLeastSignificantBits());
        bb.putLong(this.getOriginalId().getMostSignificantBits());
        bb.putLong(this.getOriginalId().getLeastSignificantBits());
        bb.putLong(this.getOCTime().getTime());
        bb.put(dataBytes);
        bb.put(schemaBytes);
        return digest.digest(bb.array());
    }

    void makeSelfReferential() {
        this.oCNode = this.localId;
        this.cNode = this;
    }

    public UUID getId() {
        return localId;
    }

    public UUID getOriginalId() {
        return this.originalId;
    }

    public UUID getOriginalInstanceId() {
        return this.originalInstanceId;
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

    public UUID getOCNodeId() {
        return this.oCNode;
    }

    public String getDataUnsafe() {
        return this.data;
    }

    public String getSchema() {
        return this.schema;
    }

    public HashMap<String, Node> getPropertiesUnsafe() {
        return this.properties;
    }

    public boolean removePropertyUnsafe(String name) {
        if (this.properties.containsKey(name)) {
            this.properties.get(name).references.get(name).remove(this); // Make sure to remove the reverse lookup
            this.properties.remove(name); // Then remove the property
            return true;
        }
        return false;
    }
    public void addPropertyUnsafe(String name, Node node) throws DuplicatePropertyException {
        if (this.properties.containsKey(name)) {
            throw new DuplicatePropertyException();
        } else {
            this.properties.put(name, node);
            System.out.println("Linked " + this.getId() + " ==[ " + name + " ]=> " + node.getId());
            if (!node.references.containsKey(name)) {
                node.references.put(name, new ArrayList<>());
            }
            node.references.get(name).add(this); // Add reverse lookup
        }
    }

    public void replacePropertyUnsafe(String name, Node node) throws DuplicatePropertyException {
        if (this.properties.containsKey(name)) {
            this.removePropertyUnsafe(name);
        }
        this.properties.put(name, node);
        System.out.println("Linked (forced) " + this.getId() + " ==[ " + name + " ]=> " + node.getId());
        if (!node.references.containsKey(name)) {
            node.references.put(name, new ArrayList<>());
        }
        node.references.get(name).add(this); // Add reverse lookup
    }

    public Node getPropertyUnsafe(String name) {
        if (this.properties.containsKey(name)) {
            return this.properties.get(name);
        } else {
            return null;
        }
    }

    public HashMap<String, ArrayList<Node>> getAllReferrersUnsafe() {
        return this.references;
    }

    public HashMap<String, Node[]> getAllReferrers(SecurityContext securityContext) {
        HashMap<String, Node[]> output = new HashMap<>();
        for (String index : this.references.keySet()) {
            Node[] nodes = this.getReferrers(securityContext, index);
            if (nodes.length > 0) {
                output.put(index, nodes);
            }
        }
        return output;
    }

    public Node[] getReferrers(SecurityContext securityContext, String name) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.READ)) {
            if (this.references.containsKey(name)) {
                Node[] nodes = this.references.get(name).toArray(new Node[0]);
                return nodes;
            } else {
                return new Node[0];
            }
        } else {
            return new Node[0];
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return Objects.equals(this.localId, node.localId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.localId);
    }

    public HashMap<String, Node> getProperties(SecurityContext securityContext) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.READ)) {
            return this.getPropertiesUnsafe();
        } else {
            return new HashMap<>();
        }
    }

    public Node getProperty(SecurityContext securityContext, String key) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.READ)) {
            return this.getPropertyUnsafe(key);
        } else {
            return null;
        }
    }

    public boolean removeProperty(SecurityContext securityContext, String name) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.WRITE)) {
            return this.removePropertyUnsafe(name);
        }
        return false;
    }

    public boolean completeUnlinkUnsafe() {
        for (String nodeCollectionKey : this.references.keySet().toArray(new String[0])) {
            Node[] nodeCollection = this.references.get(nodeCollectionKey).toArray(new Node[0]);
            for (Node node : nodeCollection) {
                node.removePropertyUnsafe(nodeCollectionKey);
            }
        }
        for (String nodeKey : this.properties.keySet().toArray(new String[0])) {
            this.removePropertyUnsafe(nodeKey);
        }
        return true;
    }

    public boolean completeUnlink(SecurityContext securityContext) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.DELETE)) {
            return this.completeUnlinkUnsafe();
        }

        return false;
    }

    public boolean hasProperty(SecurityContext securityContext, String name) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.READ)) {
            return this.properties.containsKey(name);
        }
        return false;
    }
    public void addProperty(SecurityContext securityContext, String name, Node node, boolean overwrite) throws DuplicatePropertyException {
        // You need WRITE permissions on *both* source and destination to connect nodes.
        // This is an important security consideration so users cannot elevate their own privileges by putting a node in a context where they would gain WRITE privileges
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.WRITE)) {
            if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), node)).contains(AuthorizedAction.WRITE)) {
                Pattern validPropName = Pattern.compile("^[a-z_][a-z0-9_]*$");
                Pattern validNumericalName = Pattern.compile("^[0-9]+$");
                if (validPropName.matcher(name).matches()) {
                    if (overwrite) {
                        this.replacePropertyUnsafe(name, node);
                    } else {
                        this.addPropertyUnsafe(name, node);
                    }
                } else if (validNumericalName.matcher(name).matches()) {
                    if (overwrite) {
                        this.replacePropertyUnsafe(name, node);
                    } else {
                        this.addPropertyUnsafe(name, node);
                    }
                } else if (name.equals("#")) {
                    int insertAt = 0;
                    for (String key : properties.keySet()) {
                        if (validNumericalName.matcher(key).matches()) {
                            if (Integer.parseInt(key) >= insertAt) {
                                insertAt = Integer.parseInt(key) + 1;
                            }
                        }
                    }
                    this.addPropertyUnsafe(String.valueOf(insertAt), node);
                } else {
                    throw new RuntimeException("Not a valid property name");
                }
            } else {
                throw new RuntimeException("Missing write perms on {" + node.getId() + "}");
            }
        } else {
            throw new RuntimeException("Missing write perms on {" + this.getId() + "}");
        }
    }

    public void addProperty(SecurityContext securityContext, String name, Node node) throws DuplicatePropertyException {
        this.addProperty(securityContext, name, node, false);
    }

    public String getData(SecurityContext securityContext) {
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), this)).contains(AuthorizedAction.READ)) {
            return this.getDataUnsafe();
        } else {
            return null;
        }
    }
}
