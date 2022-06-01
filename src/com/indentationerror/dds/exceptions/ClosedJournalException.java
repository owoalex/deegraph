package com.indentationerror.dds.exceptions;

import com.indentationerror.dds.database.JournalSegment;

public class ClosedJournalException extends Exception {
    public ClosedJournalException(JournalSegment journal) {
        super("Journal {" + journal.getId() + "} has been closed, and cannot be written to");
    }
}
