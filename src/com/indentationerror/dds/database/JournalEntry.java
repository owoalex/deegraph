package com.indentationerror.dds.database;

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
}
