package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import com.indentationerror.dds.formats.WUUID;

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;

public class NewNodeJournalEntry extends JournalEntry {


    private UUID localId;
    private WUUID globalId;
    private Date oCTime;
    private UUID cNode;
    private WUUID oCNode;
    private String data;
    private String schema;

    public NewNodeJournalEntry(Node node) {
        super(node.getCTime());
        this.localId = node.getId();
        this.globalId = node.getGlobalId();
        if (node.getCNode() != null) {
            this.cNode = node.getCNode().getId();
        }
        this.oCNode = node.getOCNodeId();
        this.data = node.getData();
        this.schema = node.getSchema();
        this.oCTime = node.getOCTime();
    }

    public NewNodeJournalEntry(UUID localId, WUUID globalId, UUID cNode, WUUID oCNode, String data, String schema, Date cTime, Date oCTime) {
        super(cTime);
        this.localId = localId;
        this.globalId = globalId;
        this.cNode = cNode;
        this.oCNode = oCNode;
        this.data = data;
        this.schema = schema;
        this.oCTime = oCTime;
    }

    public void replayOn(DatabaseInstance databaseInstance) throws DuplicateNodeStoreException {
        if (databaseInstance.getNode(this.globalId) != null) { // We already have this exact node in the database!
            throw new DuplicateNodeStoreException(databaseInstance.getNode(this.globalId));
        }
        while (databaseInstance.getNode(this.localId) != null) { // Hmmm, UUID collision, but not the same origin. This can happen for a few reasons - let's just randomize the last bit to get a new UUID for local purposed
            this.localId = new UUID(this.localId.getMostSignificantBits(), new SecureRandom().nextLong());
        }
        Node node = new Node(this.localId, this.globalId, databaseInstance.getNode(this.cNode), this.oCNode, this.data, this.schema, this.timestamp, this.oCTime);
        databaseInstance.registerNode(node);
    }

    public UUID getId() {
        return localId;
    }

    public WUUID getGlobalId() {
        return globalId;
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

    public WUUID getOCNodeId() {
        return oCNode;
    }

    public String getData() {
        return data;
    }

    public String getSchema() {
        return schema;
    }
}
