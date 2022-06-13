package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.MissingNodeException;
import com.indentationerror.dds.formats.WUUID;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class RemoveRelationJournalEntry extends JournalEntry {
    private UUID referrerLocalId;
    private WUUID referrerGlobalId;
    private String referenceName;
    private UUID subjectLocalId;
    private WUUID subjectGlobalId;

    public RemoveRelationJournalEntry(Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.referrerLocalId = referrer.getId();
        this.referrerGlobalId = referrer.getGlobalId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
        this.subjectGlobalId = subject.getGlobalId();
    }

    public RemoveRelationJournalEntry(Date dTime, UUID referrerLocalId, WUUID referrerGlobalId, String referenceName, UUID subjectLocalId, WUUID subjectGlobalId) {
        super(dTime);
        this.referrerLocalId = referrerLocalId;
        this.referrerGlobalId = referrerGlobalId;
        this.referenceName = referenceName;
        this.subjectLocalId = subjectLocalId;
        this.subjectGlobalId = subjectGlobalId;
    }

    @Override
    public void replayOn(GraphDatabase graphDatabase) throws MissingNodeException {
        Node referrerNode = null;
        if (this.referrerLocalId != null) { // This might be set to null if this is a foreign journal, as they are only allowed to use the less efficient global id
            referrerNode = graphDatabase.getNode(this.referrerLocalId);
        }
        if (referrerNode == null) { // This is a foreign journal being replayed OR we couldn't find the node, we need to look for the global id which should always match
            referrerNode = graphDatabase.getNode(this.referrerGlobalId);
        }
        if (referrerNode == null) {
            throw new MissingNodeException(this.referrerGlobalId);
        } else { // We found it!
            Node subjectNode = null;
            if (this.subjectLocalId != null) {
                subjectNode = graphDatabase.getNode(this.subjectLocalId);
            }
            if (subjectNode == null) {
                subjectNode = graphDatabase.getNode(this.subjectGlobalId);
            }
            if (subjectNode == null) {
                throw new MissingNodeException(this.subjectGlobalId);
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
                        throw new MissingNodeException(this.subjectGlobalId);
                    }
                }
            }
        }
    }
}
