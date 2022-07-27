package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import org.json.JSONObject;

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;

public class NewNodeJournalEntry extends JournalEntry {


    private UUID localId;
    private UUID originalInstanceId;
    private UUID originalId;
    private Date oCTime;
    private UUID cNode;
    private UUID oCNode;
    private String data;
    private String schema;

    private TrustBlock trustRoot;

    public NewNodeJournalEntry(Node node) {
        super(node.getCTime());
        this.localId = node.getId();
        this.originalInstanceId = node.getOriginalInstanceId();
        this.originalId = node.getOriginalId();
        if (node.getCNode() != null) {
            this.cNode = node.getCNode().getId();
        }
        this.oCNode = node.getOCNodeId();
        this.data = node.getDataUnsafe();
        this.schema = node.getSchema();
        this.oCTime = node.getOCTime();
        this.trustRoot = node.getTrustRoot();
    }

    public NewNodeJournalEntry(UUID localId, UUID originalId, UUID originalInstanceId, UUID cNode, UUID oCNode, String data, String schema, Date cTime, Date oCTime, TrustBlock trustRoot) {
        super(cTime);
        this.localId = localId;
        this.originalInstanceId = originalInstanceId;
        this.originalId = originalId;
        this.cNode = cNode;
        this.oCNode = oCNode;
        this.data = data;
        this.schema = schema;
        this.oCTime = oCTime;
        this.trustRoot = trustRoot;
    }

    @Override
    public void replayOn(GraphDatabase graphDatabase) throws DuplicateNodeStoreException {
        if (graphDatabase.getNodeUnsafe(this.originalId, this.originalInstanceId) != null) { // We already have this exact node in the database!
            throw new DuplicateNodeStoreException(graphDatabase.getNodeUnsafe(this.originalId, this.originalInstanceId));
        }
        while (graphDatabase.getNodeUnsafe(this.localId) != null) { // Hmmm, UUID collision, but not the same origin. This can happen for a few reasons - let's just randomize the last bit to get a new UUID for local purposed
            this.localId = new UUID(this.localId.getMostSignificantBits(), new SecureRandom().nextLong());
        }
        Node node = new Node(graphDatabase, this.localId, this.originalId, this.originalInstanceId, graphDatabase.getNodeUnsafe(this.cNode), this.oCNode, this.data, this.schema, this.timestamp, this.oCTime, this.trustRoot);
        graphDatabase.registerNodeUnsafe(node);
    }

    public UUID getId() {
        return localId;
    }

    public UUID getOriginalInstanceId() {
        return originalInstanceId;
    }

    public UUID getOriginalId() {
        return originalId;
    }
    public Date getCTime() {
        return this.getTimestamp();
    }

    public Date getOCTime() {
        return oCTime;
    }

    public UUID getCNodeId() {
        return cNode;
    }

    public UUID getOCNodeId() {
        return oCNode;
    }

    public String getData() {
        return data;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public JSONObject asJson() {
        JSONObject out = new JSONObject();
        out.put("local_id", this.localId);
        out.put("original_id", this.originalId);
        out.put("original_instance_id", this.originalInstanceId);
        out.put("data", this.data);
        out.put("schema", this.schema);
        out.put("creator_node", this.cNode);
        out.put("original_creator", this.oCNode);
        out.put("created", JournalEntry.formatDate(this.getCTime()));
        out.put("first_appeared", JournalEntry.formatDate(this.oCTime));
        out.put("trust_chain", this.trustRoot.toJson());
        return  out;
    }
}
