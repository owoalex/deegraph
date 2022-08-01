package org.deegraph.database;

import org.deegraph.exceptions.MissingNodeException;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class RemoveRelationJournalEntry extends JournalEntry {
    private UUID referrerLocalId;
    private String referenceName;
    private UUID subjectLocalId;
    public RemoveRelationJournalEntry(Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.referrerLocalId = referrer.getId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
    }

    public RemoveRelationJournalEntry(Date dTime, UUID referrerLocalId, String referenceName, UUID subjectLocalId) {
        super(dTime);
        this.referrerLocalId = referrerLocalId;
        this.referenceName = referenceName;
        this.subjectLocalId = subjectLocalId;
    }

    @Override
    public boolean replayOn(GraphDatabase graphDatabase, Node source) throws MissingNodeException {
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
                if (this.referenceName.equals("#")) { // Special wildcard remove for array-nodes, we don't care about where in the array it is
                    HashMap<String, Node> properties = referrerNode.getPropertiesUnsafe();
                    for (String key : properties.keySet()) {
                        if (key.matches("^[0-9]+$")) {
                            if (properties.get(key).equals(subjectNode)) {
                                referrerNode.removePropertyUnsafe(key);
                            }
                        }
                    }
                } else {
                    Node matchingSubject = referrerNode.getPropertyUnsafe(this.referenceName);
                    if (matchingSubject != null) {
                        if (matchingSubject.equals(subjectNode)) { // Only remove the node if the property matches
                            referrerNode.removePropertyUnsafe(this.referenceName);
                        }
                    } else {
                        throw new MissingNodeException(this.subjectLocalId);
                    }
                }
            }
        }
        return false;
    }
}
