package com.indentationerror.dds;

public class ClosedJournalException extends Exception {
    public ClosedJournalException(JournalSegment journal) {
        super("Journal {" + journal.getId() + "} has been closed, and cannot be written to");
    }
}
