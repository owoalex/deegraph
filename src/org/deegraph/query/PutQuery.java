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
                    at = parsedQuery.poll();
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
            into = at.substring(0,at.lastIndexOf("/"));
            as = at.substring(at.lastIndexOf("/"));
            System.out.println(into + " : " + as);
        }

        if (into != null) {
            if (as == null) {
                as = "#";
            }
            RelativeNodePath intoRelPath = new RelativeNodePath(into);
            if (intoRelPath != null) {
                Node[] toNodes = intoRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe());
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
