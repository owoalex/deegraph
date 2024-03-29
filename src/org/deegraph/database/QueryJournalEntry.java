package org.deegraph.database;

import org.deegraph.exceptions.ClosedJournalException;
import org.deegraph.exceptions.DuplicatePropertyException;
import org.deegraph.query.*;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;
import java.util.UUID;

public class QueryJournalEntry extends JournalEntry {
    private String query;
    private UUID actor;

    private Date timestamp;

    public QueryJournalEntry(String query, Node actor) {
        this(query, actor.getId());
    }

    public QueryJournalEntry(String query, UUID actor) {
        this.actor = actor;
        this.query = query;
        this.timestamp = new Date();
    }

    public UUID getActor() {
        return actor;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public JSONObject asJson() {
        JSONObject out = new JSONObject();
        out.put("type", "QUERY");
        out.put("actor_id", this.actor);
        out.put("query", this.query);
        out.put("timestamp", JournalEntry.formatDate(this.timestamp));
        return out;
    }

    public static JournalEntry fromJson(JSONObject input) {
        UUID actor = UUID.fromString(input.getString("actor_id"));
        String query = input.getString("query");
        QueryJournalEntry qje = new QueryJournalEntry(query, actor);
        qje.timestamp = JournalEntry.fromFormattedDate(input.getString("timestamp"));
        return qje;
    }
    @Override
    public boolean replayOn(GraphDatabase graphDatabase, Node source) throws ParseException {
        Query q = Query.fromString(this.query, graphDatabase.getNodeUnsafe(this.actor));
        //System.out.println("Replaying query \"" + this.query.trim() + "\"");
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
                case INSERT:
                    ((InsertQuery) q).runInsertQuery(graphDatabase);
                    break;
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (QueryException e) {
            throw new RuntimeException(e);
        } catch (DuplicatePropertyException e) {
            throw new RuntimeException(e);
        } catch (ClosedJournalException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
