package org.deegraph.exceptions;

public class UnvalidatedJournalSegment extends Exception {
    public UnvalidatedJournalSegment() {
        super("Could not validate journal segment");
    }

    public UnvalidatedJournalSegment(String message) {
        super(message);
    }
}
