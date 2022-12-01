package org.deegraph.database;

import org.deegraph.exceptions.DuplicatePropertyException;
import org.deegraph.exceptions.MissingNodeException;
import org.json.JSONObject;

import java.util.Date;
import java.util.UUID;

public class AddRelationJournalEntry extends JournalEntry {
    private UUID actor;
    private UUID referrerLocalId;
    private String referenceName;
    private UUID subjectLocalId;

    public AddRelationJournalEntry(Node actor, Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.actor = actor.getId();
        this.referrerLocalId = referrer.getId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
    }

    public AddRelationJournalEntry(Date dTime, UUID actor, UUID referrerLocalId, String referenceName, UUID subjectLocalId) {
        super(dTime);
        this.actor = actor;
        this.referrerLocalId = referrerLocalId;
        this.referenceName = referenceName;
        this.subjectLocalId = subjectLocalId;
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
            if (subjectNode == null) {
                throw new MissingNodeException(this.subjectLocalId);
            } else {
                try {
                    Node existingSubject = referrerNode.getPropertyUnsafe(this.referenceName);
                    if (existingSubject != null) {
                        if (existingSubject.getOCTime().before(subjectNode.getOCTime())) { // Only replace the node if the one given is newer - we almost always want the most recent data
                            referrerNode.removeProperty(new SecurityContext(graphDatabase, actorNode), this.referenceName);
                            referrerNode.addProperty(new SecurityContext(graphDatabase, actorNode), this.referenceName, subjectNode);
                        }
                    } else {
                        referrerNode.addProperty(new SecurityContext(graphDatabase, actorNode), this.referenceName, subjectNode);
                    }
                } catch (DuplicatePropertyException e) { // Really shouldn't happen -> would only happen if a concurrent modification happens
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    @Override
    public JSONObject asJson() {
        JSONObject out = new JSONObject();
        out.put("type", "ADD_RELATION");
        out.put("actor_id", this.actor);
        out.put("referrer_id", this.referrerLocalId);
        out.put("key", this.referenceName);
        out.put("subject_id", this.subjectLocalId);
        out.put("timestamp", JournalEntry.formatDate(this.timestamp));
        return out;
    }

    public static JournalEntry fromJson(JSONObject input) {
        UUID actor = UUID.fromString(input.getString("actor_id"));
        UUID referrerLocalId = UUID.fromString(input.getString("referrer_id"));
        UUID subjectLocalId = UUID.fromString(input.getString("subject_id"));
        String referenceName = input.getString("key");
        Date dTime = JournalEntry.fromFormattedDate(input.getString("timestamp"));
        JournalEntry je = new AddRelationJournalEntry(dTime, actor, referrerLocalId, referenceName, subjectLocalId);
        return je;
    }
}
