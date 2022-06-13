package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import com.indentationerror.dds.exceptions.MissingNodeException;

import java.text.ParseException;
import java.util.Date;

public abstract class JournalEntry {
    protected Date timestamp;
    public JournalEntry() {
        this.timestamp = new Date();
    }

    public JournalEntry(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void replayOn(GraphDatabase graphDatabase) throws MissingNodeException, DuplicateNodeStoreException, ParseException {}
}
