package org.deegraph.query;

import org.deegraph.conditions.Condition;
import org.deegraph.database.*;

import java.text.ParseException;
import java.util.*;

public class SelectQuery extends Query {
    protected SelectQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Map<UUID, Map<String, Map<AbsoluteNodePath, Node>>> runSelectQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
        if (this.queryType != QueryType.SELECT) {
            throw new NoSuchMethodException();
        }

        ArrayList<String> requestedProperties = new ArrayList<>();
        boolean escape = false;
        String current = null;
        while (!escape) {
            current = parsedQuery.poll();
            requestedProperties.add(current);
            current = parsedQuery.poll();
            if (current == null) {
                break;
            }
            escape = !(current.trim().equals(","));
        }
        Condition condition = null;
        String schemaLimit = null;
        String fromLimit = null;
        ArrayList<Node> candidateNodes = null;
        escape = (current == null);
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "FROM":
                    fromLimit = parsedQuery.poll();
                    break;
                case "INSTANCEOF":
                    schemaLimit = parsedQuery.poll();
                    break;
                case "WHERE":
                    condition = parseConditionFromRemaining(graphDatabase);
                    break;
                default:
                    throw new ParseException("'" + current + "' is not a valid control word", 0);
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        Map<UUID, Map<String, Map<AbsoluteNodePath, Node>>> outputMaps = new HashMap<>();

        if (fromLimit == null) {
            candidateNodes = new ArrayList<>();
            candidateNodes.add(this.actor);
        } else {
            RelativeNodePath fromRelPath = new RelativeNodePath(fromLimit);
            if (fromRelPath != null) {
                candidateNodes = new ArrayList<>(Arrays.asList(fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe())));
            } else {
                throw new RuntimeException("Error parsing '" + fromLimit + "' as path") ;
            }
        }

        if (schemaLimit != null) { // Filter by INSTANCEOF first, as it's quite a cheap operation
            if (schemaLimit.startsWith("\"") && schemaLimit.endsWith("\"")) {
                schemaLimit = schemaLimit.substring(1, schemaLimit.length() - 1);
            }
            ArrayList<Node> newCandidates = new ArrayList<>();
            for (Node candidate : candidateNodes) {
                if (schemaLimit.equals(candidate.getSchema())) {
                    newCandidates.add(candidate);
                }
            }
            candidateNodes = newCandidates;
        }

        if (condition != null) {
            ArrayList<Node> newCandidates = new ArrayList<>();
            for (Node candidate : candidateNodes) {
                if (condition.eval(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, candidate))) {
                    newCandidates.add(candidate);
                }
            }
            candidateNodes = newCandidates;
        }

        for (Node candidate : candidateNodes) { // Expand the output to provide every node
            Map<String, Map<AbsoluteNodePath, Node>> resultRepresentation = new HashMap<>();
            for (String property : requestedProperties) {
                Map<AbsoluteNodePath, Node> matches = new RelativeNodePath(property).getMatchingNodeMap(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, candidate), graphDatabase.getAllNodesUnsafe());
                resultRepresentation.put(property, matches);
            }
            outputMaps.put(candidate.getId(), resultRepresentation);
        }
        return outputMaps;
    }

}
