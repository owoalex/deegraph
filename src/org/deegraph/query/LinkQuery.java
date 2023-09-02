package org.deegraph.query;

import org.deegraph.database.*;
import org.deegraph.exceptions.ClosedJournalException;
import org.deegraph.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class LinkQuery extends Query {
    protected LinkQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public boolean runLinkQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException, ClosedJournalException {
        if (this.queryType != QueryType.LINK) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        boolean overwrite = false;
        String linkName = "#"; // Wildcard for inserting at the next available numbered property
        String toName = "";
        Node[] valueNodes = new RelativeNodePath(current).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor), graphDatabase.getAllNodesUnsafe());
        Node valueNode = (valueNodes.length == 1) ? valueNodes[0] : null;
        Node toNode = null;
        current = parsedQuery.poll();
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "TO":
                case "OF":
                    toName = parsedQuery.poll();
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

        String[] linkNameComponents = linkName.split("/");
        if (linkNameComponents.length > 1) {
            linkName = linkNameComponents[linkNameComponents.length - 1];
            linkNameComponents[linkNameComponents.length - 1] = "";
            ArrayList<String> toComponents = new ArrayList<>();
            toComponents.addAll(List.of(toName.split("/")));
            toComponents.addAll(List.of(linkNameComponents));
            toComponents.removeAll(Arrays.asList("", null));
            toName = String.join("/", toComponents);
        }

        RelativeNodePath fromRelPath = new RelativeNodePath(toName);
        if (fromRelPath != null) {
            Node[] toNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor), graphDatabase.getAllNodesUnsafe());
            toNode = (toNodes.length == 1) ? toNodes[0] : null;
        } else {
            throw new RuntimeException("Error parsing '" + toName + "' as path") ;
        }

        if (toNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_TO_PARAMETER);
        }

        if (valueNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        String parsedLinkName = toNode.addProperty(new SecurityContext(graphDatabase, this.actor), linkName, valueNode, overwrite);
        graphDatabase.getOpenJournal().registerEntry(new AddRelationJournalEntry(this.actor, toNode, parsedLinkName, valueNode));
        //System.out.println("Attempting link");

        return true;
    }
}
