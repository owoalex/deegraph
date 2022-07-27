package com.indentationerror.dds.query;

import com.indentationerror.dds.database.*;
import com.indentationerror.dds.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;

public class UnlinkQuery extends Query {
    protected UnlinkQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public boolean runUnlinkQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException {
        if (this.queryType != QueryType.UNLINK) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current;
        Node childNode = null;
        Node parentNode = null;
        String firstString = parsedQuery.poll();
        current = parsedQuery.poll();
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "FROM":
                case "OF":
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    if (fromRelPath != null) {
                        Node[] parentNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe());
                        parentNode = (parentNodes.length == 1) ? parentNodes[0] : null;
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path") ;
                    }
                    break;
                default:
                    throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD, "'" + current + "' is not a valid control word");
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        if (parentNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_TO_PARAMETER);
        }

        RelativeNodePath fromRelPath = new RelativeNodePath(firstString);
        if (fromRelPath != null) {
            Node[] parentNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, parentNode), graphDatabase.getAllNodesUnsafe());
            childNode = (parentNodes.length == 1) ? parentNodes[0] : null;
        } else {
            throw new RuntimeException("Error parsing '" + firstString + "' as path") ;
        }
        if (childNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        if (parentNode.hasProperty(new SecurityContext(graphDatabase, this.actor), firstString)) {
            return parentNode.removeProperty(new SecurityContext(graphDatabase, this.actor), firstString);
        } else {
            boolean oneFound = false;
            HashMap<String, Node> props = parentNode.getProperties(new SecurityContext(graphDatabase, this.actor));
            for (String key : props.keySet()) {
                if (props.get(key).equals(childNode)) {
                    parentNode.removeProperty(new SecurityContext(graphDatabase, this.actor), firstString);
                    oneFound = true;
                }
            }
            return oneFound;
        }
    }
}
