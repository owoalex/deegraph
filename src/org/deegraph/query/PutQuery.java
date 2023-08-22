package org.deegraph.query;

import org.deegraph.database.*;
import org.deegraph.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.Locale;

public class PutQuery extends Query {
    protected PutQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Node runPutQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException {
        if (this.queryType != QueryType.PUT) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        String data = null;
        String at = null;
        String into = null;
        String as = null;
        String schema = null;
        boolean overwrite = true;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "URI":
                case "URL":
                    data = parsedQuery.poll();
                    if (data.startsWith("\"") && data.endsWith("\"")) {
                        data = data.substring(1, data.length() - 1);
                    }
                    break;
                case "SCHEMA":
                    schema = parsedQuery.poll();
                    if (schema.startsWith("\"") && schema.endsWith("\"")) {
                        schema = schema.substring(1, schema.length() - 1);
                    }
                    break;
                case "AT":
                    if (into != null || as != null) {
                        throw new QueryException(QueryExceptionCode.INVALID_COMBINATION, "Cannot assign 'AT' when 'INTO' or 'AS' is already set");
                    }
                    at = parsedQuery.poll();
                    break;
                case "INTO":
                    if (at != null) {
                        throw new QueryException(QueryExceptionCode.INVALID_COMBINATION, "Cannot assign 'INTO' when 'AT' is already set");
                    }
                    into = parsedQuery.poll();
                    break;
                case "AS":
                    if (at != null) {
                        throw new QueryException(QueryExceptionCode.INVALID_COMBINATION, "Cannot assign 'AS' when 'AT' is already set");
                    }
                    as = parsedQuery.poll();
                    break;
                case "SAFE":
                    overwrite = false;
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

        if (at != null) {
            if (at.lastIndexOf("/") == -1) {
                as = at;
            } else {
                into = at.substring(0, at.lastIndexOf("/"));
                as = at.substring(at.lastIndexOf("/") + 1);
                System.out.println(into + " : " + as);
            }
        }

        if (into != null) {
            if (as == null) {
                as = "#";
            }
            RelativeNodePath intoRelPath = new RelativeNodePath(into);
            if (intoRelPath != null) {
                Node[] toNodes = intoRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor), graphDatabase.getAllNodesUnsafe());
                if (toNodes.length == 1) {
                    toNodes[0].addProperty(new SecurityContext(graphDatabase, this.actor), as, newNode, overwrite);
                }
            } else {
                throw new RuntimeException("Error parsing '" + into + "' as path");
            }

        }
        //System.out.println(newNode.getCNode().getId());

        return newNode;
    }
}
