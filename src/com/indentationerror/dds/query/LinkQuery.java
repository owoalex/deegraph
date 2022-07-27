package com.indentationerror.dds.query;

import com.indentationerror.dds.database.*;
import com.indentationerror.dds.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;

public class LinkQuery extends Query {
    protected LinkQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public boolean runLinkQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException {
        if (this.queryType != QueryType.LINK) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        boolean overwrite = false;
        String linkName = "#"; // Wildcard for inserting at the next available numbered property
        Node[] valueNodes = new RelativeNodePath(current).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe());
        Node valueNode = (valueNodes.length == 1) ? valueNodes[0] : null;
        Node toNode = null;
        current = parsedQuery.poll();
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "TO":
                case "OF":
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    if (fromRelPath != null) {
                        Node[] toNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe());
                        toNode = (toNodes.length == 1) ? toNodes[0] : null;
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path") ;
                    }
                    break;
                case "AS":
                    linkName = parsedQuery.poll();
                    break;
                case "OVERWRITE":
                case "REPLACE":
                case "FORCE":
                    overwrite = true;
                    break;
                default:
                    throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD, "'" + current + "' is not a valid control word");
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        if (toNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_TO_PARAMETER);
        }

        if (valueNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        toNode.addProperty(new SecurityContext(graphDatabase, this.actor), linkName, valueNode, overwrite);
        //System.out.println("Attempting link");

        return true;
    }
}
