package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicatePropertyException;
import com.indentationerror.dds.exceptions.MissingNodeException;
import com.indentationerror.dds.formats.WUUID;

import java.util.Date;
import java.util.UUID;

public class AddRelationJournalEntry extends JournalEntry {
    private UUID referrerLocalId;
    private WUUID referrerGlobalId;
    private String referenceName;
    private UUID subjectLocalId;
    private WUUID subjectGlobalId;

    public AddRelationJournalEntry(Node referrer, String referenceName, Node subject) {
        super(new Date());
        this.referrerLocalId = referrer.getId();
        this.referrerGlobalId = referrer.getGlobalId();
        this.referenceName = referenceName;
        this.subjectLocalId = subject.getId();
        this.subjectGlobalId = subject.getGlobalId();
    }

    public AddRelationJournalEntry(Date dTime, UUID referrerLocalId, WUUID referrerGlobalId, String referenceName, UUID subjectLocalId, WUUID subjectGlobalId) {
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
