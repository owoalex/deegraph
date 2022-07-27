package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicatePropertyException;
import com.indentationerror.dds.exceptions.MissingNodeException;

import java.util.Date;
import java.util.UUID;

public class AddRelationJournalEntry extends JournalEntry {
    private UUID referrerLocalId;
    private String referenceName;
    private UUID subjectLocalId;

    public AddRelationJournalEntry(Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.referrerLocalId = referrer.getId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
    }

    public AddRelationJournalEntry(Date dTime, UUID referrerLocalId, String referenceName, UUID subjectLocalId) {
        super(dTime);
        this.referrerLocalId = referrerLocalId;
        this.referenceName = referenceName;
        this.subjectLocalId = subjectLocalId;
    }

    @Override
    public void replayOn(GraphDatabase graphDatabase) throws MissingNodeException {
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
                            referrerNode.removePropertyUnsafe(this.referenceName);
                            referrerNode.addPropertyUnsafe(this.referenceName, subjectNode);
                        }
                    } else {
                        referrerNode.addPropertyUnsafe(this.referenceName, subjectNode);
                    }
                } catch (DuplicatePropertyException e) { // Really shouldn't happen -> would only happen if a concurrent modification happens
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
