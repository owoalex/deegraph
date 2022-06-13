package com.indentationerror.dds.database;

import com.indentationerror.dds.query.Query;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class RawQueryJournalEntry extends JournalEntry {
    private String query;
    private UUID actor;

    public RawQueryJournalEntry(String query, Node actor) {
        this(query, actor.getId());
    }

    public RawQueryJournalEntry(String query, UUID actor) {
        this.actor = actor;
        this.query = query;
    }
    @Override
    public void replayOn(GraphDatabase graphDatabase) throws ParseException {
        Query q = new Query(this.query, graphDatabase.getNode(this.actor));
        try {
            q.runGrantQuery(graphDatabase);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
