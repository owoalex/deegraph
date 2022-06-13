package com.indentationerror.dds.database;

import com.indentationerror.dds.formats.WUUID;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class NodeArchiveJournalEntry extends JournalEntry {
    private UUID localId;
    private WUUID globalId;

    public NodeArchiveJournalEntry(Node node) {
        super(new Date());
        this.localId = node.getId();
        this.globalId = node.getGlobalId();
    }

    public NodeArchiveJournalEntry(UUID localId, WUUID globalId, Date dTime) {
        super(dTime);
        this.localId = localId;
        this.globalId = globalId;
    }

    public UUID getLocalId() {
        return localId;
    }

    public WUUID getGlobalId() {
        return globalId;
    }

    @Override
    public void replayOn(GraphDatabase graphDatabase) {
        Node node = null;
        if (this.localId != null) { // This might be set to null if this is a foreign journal, as they are only allowed to use the less efficient global id
            node = graphDatabase.getNode(this.localId);
        }
        if (node == null) { // This is a foreign journal being replayed OR we couldn't find the node, we need to look for the global id which should always match
            node = graphDatabase.getNode(this.globalId);
        }
        if (node != null) { // We found it!
            HashMap<String, ArrayList<Node>> references = node.getAllReferrers();
            for (String key : references.keySet()) { // Remove references to node first
                for (Node referrer : references.get(key)) {
                    referrer.removePropertyUnsafe(key);
                }
            }
            // TODO: check perms here
            graphDatabase.getBacking().unregisterNode(node); // Now remove it from the node directory and let the GC delete it
        }
    }
}
