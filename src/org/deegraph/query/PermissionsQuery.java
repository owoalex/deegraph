package org.deegraph.query;

import org.deegraph.database.*;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;

public class PermissionsQuery extends Query {
    protected PermissionsQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public AuthorizedAction[] runPermissionsQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, ParseException {
        if (this.queryType != QueryType.PERMISSIONS) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;

        Node asNode = this.actor;
        String onNodePath = null;
        Node onNode = null;

        if (parsedQuery.size() == 0) {
            throw new ParseException("Empty PERMISSIONS query", 0);
        }

        String current = parsedQuery.poll();
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "ON":{
                    onNodePath = parsedQuery.poll();
                    break;
                }
                case "AS": {
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    if (fromRelPath != null) {
                        Node[] asNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor), graphDatabase.getAllNodesUnsafe());
                        asNode = (asNodes.length == 1) ? asNodes[0] : null;
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path");
                    }
                    break;
                }
                default:
                    throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD, "'" + current + "' is not a valid control word");
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        if (onNodePath != null) {
            RelativeNodePath fromRelPath = new RelativeNodePath(onNodePath);
            if (fromRelPath != null) {
                Node[] onNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(asNode, null, this.actor), graphDatabase.getAllNodesUnsafe());
                onNode = (onNodes.length == 1) ? onNodes[0] : null;
            } else {
                throw new RuntimeException("Error parsing '" + onNodePath + "' as path");
            }
        }

        if (onNode != null && asNode != null) {
            if (Arrays.asList(graphDatabase.getPermsOnNode(this.actor, asNode)).contains(AuthorizedAction.READ)) {
                return graphDatabase.getPermsOnNode(asNode, onNode);
            } else {
                throw new RuntimeException("Missing read permissions for actor");
            }
        } else {
            throw new RuntimeException("Cannot calculate permissions on null");
        }
    }
}
