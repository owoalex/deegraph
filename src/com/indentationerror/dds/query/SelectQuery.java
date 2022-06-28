package com.indentationerror.dds.query;

import com.indentationerror.dds.conditions.Condition;
import com.indentationerror.dds.database.*;
import com.indentationerror.dds.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.*;

public class SelectQuery extends Query {
    protected SelectQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public List<Map<String, Node[]>> runSelectQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
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
        ArrayList<Node> candidateNodes = new ArrayList<>(Arrays.asList(graphDatabase.getAllNodes()));
        escape = (current == null);
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "FROM":
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    if (fromRelPath != null) {
                        candidateNodes = new ArrayList<>(Arrays.asList(fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes())));
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path") ;
                    }
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

        if (condition != null) { // Filter based on condition after, this is potentially a very expensive operation
            ArrayList<Node> newCandidates = new ArrayList<>();
            for (Node candidate : candidateNodes) {
                if (condition.eval(new NodePathContext(this.actor, candidate))) {
                    newCandidates.add(candidate);
                }
            }
            candidateNodes = newCandidates;
        }

        ArrayList<Map<String, Node[]>> outputMaps = new ArrayList<>();
        for (Node candidate : candidateNodes) { // Expand the output to provide every node
            HashMap<String, Node[]> resultRepresentation = new HashMap<>();
            for (String property : requestedProperties) {
                Node[] matches = new RelativeNodePath(property).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, candidate), graphDatabase.getAllNodes());
                resultRepresentation.put(property, matches);
            }
            outputMaps.add(resultRepresentation);
        }
        return outputMaps;
    }

}
