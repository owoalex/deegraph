package org.deegraph.query;

import org.deegraph.database.*;
import org.deegraph.exceptions.DuplicatePropertyException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Locale;

public class ConstructQuery extends Query {
    protected ConstructQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public boolean runConstructQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException {
        if (this.queryType != QueryType.CONSTRUCT) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        String data = null;
        String schema = null;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "URI":
                case "URL":
                    data = parsedQuery.poll();
                    break;
                case "SCHEMA":
                    schema = parsedQuery.poll();
                    break;
                default:
                    throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD, "'" + current + "' is not a valid control word");
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        Node newNode = graphDatabase.newNode(data, this.actor, schema);
        System.out.println(newNode.getCNode().getId());

        return true;
    }
}
