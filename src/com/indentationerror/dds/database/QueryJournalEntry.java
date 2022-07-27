package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicatePropertyException;
import com.indentationerror.dds.query.*;

import java.text.ParseException;
import java.util.UUID;

public class QueryJournalEntry extends JournalEntry {
    private String query;
    private UUID actor;

    public QueryJournalEntry(String query, Node actor) {
        this(query, actor.getId());
    }

    public QueryJournalEntry(String query, UUID actor) {
        this.actor = actor;
        this.query = query;
    }

    public UUID getActor() {
        return actor;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public void replayOn(GraphDatabase graphDatabase) throws ParseException {
        Query q = Query.fromString(this.query, graphDatabase.getNodeUnsafe(this.actor));
        try {
            switch (q.getQueryType()) {
                case GRANT:
                    ((GrantQuery) q).runGrantQuery(graphDatabase);
                    break;
                case LINK:
                    ((LinkQuery) q).runLinkQuery(graphDatabase);
                    break;
                case UNLINK:
                    ((UnlinkQuery) q).runUnlinkQuery(graphDatabase);
                    break;
                case DELETE:
                    ((DeleteQuery) q).runDeleteQuery(graphDatabase);
                    break;
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (QueryException e) {
            throw new RuntimeException(e);
        } catch (DuplicatePropertyException e) {
            throw new RuntimeException(e);
        }
    }
}
