package org.deegraph.database;

import org.deegraph.exceptions.MissingNodeException;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class RemoveRelationJournalEntry extends JournalEntry {
    private UUID actor;
    private UUID referrerLocalId;
    private String referenceName;
    private UUID subjectLocalId;
    public RemoveRelationJournalEntry(Node actor, Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.actor = actor.getId();
        this.referrerLocalId = referrer.getId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
    }

    public RemoveRelationJournalEntry(Node actor, Node referrer, String referenceName) {
        super(new Date());
        this.actor = actor.getId();
        this.referrerLocalId = referrer.getId();
        this.referenceName = referenceName;
        this.subjectLocalId = null;
    }

    public RemoveRelationJournalEntry(Date dTime, UUID actor, UUID referrerLocalId, String referenceName, UUID subjectLocalId) {
        super(dTime);
        this.actor = actor;
        this.referrerLocalId = referrerLocalId;
        this.referenceName = referenceName;
        this.subjectLocalId = subjectLocalId;
    }

    public RemoveRelationJournalEntry(Date dTime, UUID actor, UUID referrerLocalId, String referenceName) {
        super(dTime);
        this.actor = actor;
        this.referrerLocalId = referrerLocalId;
        this.referenceName = referenceName;
        this.subjectLocalId = null;
    }

    @Override
    public boolean replayOn(GraphDatabase graphDatabase, Node source) throws MissingNodeException {
        Node actorNode = null;
        if (this.actor != null) {
            actorNode = graphDatabase.getNodeUnsafe(this.actor);
        }
        if (actorNode == null) {
            throw new MissingNodeException(this.actor);
        }

        Node referrerNode = null;
        if (this.referrerLocalId != null) { // This might be set to null if this is a foreign journal, as they are only allowed to use the less efficient global id
            referrerNode = graphDatabase.getNodeUnsafe(this.referrerLocalId);
        }
        if (referrerNode == null) {
            throw new MissingNodeException(this.referrerLocalId);
        } else { // We found it!
            Node subjectNode = null;
            if (this.subjectLocalId != null) {
                subjectNode = graphDatabase.getNodeUnsafe(this.subjectLocalId);
            }
            if (this.referenceName.equals("#")) { // Special wildcard remove for array-nodes, we don't care about where in the array it is
                HashMap<String, Node> properties = referrerNode.getPropertiesUnsafe();
                for (String key : properties.keySet()) {
                    if (key.matches("^[0-9]+$")) {
                        if (subjectNode == null || properties.get(key).equals(subjectNode)) {
                            referrerNode.removeProperty(new SecurityContext(graphDatabase, actorNode), key);
                        }
                    }
                }
            } else {
                Node matchingSubject = referrerNode.getPropertyUnsafe(this.referenceName);
                if (matchingSubject != null) {
                    if (subjectNode == null || matchingSubject.equals(subjectNode)) { // Only remove the node if the property matches
                        referrerNode.removeProperty(new SecurityContext(graphDatabase, actorNode), this.referenceName);
                    }
                } else {
                    throw new MissingNodeException(this.subjectLocalId);
                }
            }
        }
        return false;
    }

    @Override
    public JSONObject asJson() {
        JSONObject out = new JSONObject();
        out.put("type", "REMOVE_RELATION");
        out.put("actor_id", this.actor);
        out.put("referrer_id", this.referrerLocalId);
        out.put("key", this.referenceName);
        if (this.subjectLocalId != null) {
            out.put("subject_id", this.subjectLocalId);
        }
        out.put("timestamp", JournalEntry.formatDate(this.timestamp));
        return out;
    }

    public static JournalEntry fromJson(JSONObject input) {
        UUID actor = UUID.fromString(input.getString("actor_id"));
        UUID referrerLocalId = UUID.fromString(input.getString("referrer_id"));
        UUID subjectLocalId = null;
        if (input.has("subject_id")) {
            subjectLocalId = UUID.fromString(input.getString("subject_id"));
        }
        String referenceName = input.getString("key");
        Date dTime = JournalEntry.fromFormattedDate(input.getString("timestamp"));
        JournalEntry je = new RemoveRelationJournalEntry(dTime, actor, referrerLocalId, referenceName, subjectLocalId);
        return je;
    }
}
