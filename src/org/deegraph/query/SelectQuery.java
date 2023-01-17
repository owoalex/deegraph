package org.deegraph.query;

import org.deegraph.conditions.Condition;
import org.deegraph.database.*;
import org.deegraph.exceptions.InvalidMetaPropertyException;
import org.deegraph.formats.Tuple;
import org.deegraph.formats.TypeCoercionUtilities;

import java.text.ParseException;
import java.util.*;

import static org.deegraph.database.NodePath.metaProp;

public class SelectQuery extends Query {
    protected SelectQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public List<Map<String, Tuple<String, String>>> runSelectQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException, InvalidMetaPropertyException {
        if (this.queryType != QueryType.SELECT) {
            throw new NoSuchMethodException();
        }

        SecurityContext securityContext = new SecurityContext(graphDatabase, this.actor);
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
        String orderBy = null;
        List<AbsoluteNodePath> candidateNodes = null;
        escape = (current == null);
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "FROM":
                    fromLimit = parsedQuery.poll();
                    break;
                case "INSTANCEOF":
                    schemaLimit = parsedQuery.poll();
                    break;
                case "ORDERBY":
                    orderBy = parsedQuery.poll();
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



        List<Map<String, Tuple<String, String>>> output = new ArrayList<>();

        if (fromLimit == null) {
            candidateNodes = new ArrayList<>();
            candidateNodes.add(new AbsoluteNodePath("{" + this.actor.getId() + "}"));
        } else {
            RelativeNodePath fromRelPath = new RelativeNodePath(fromLimit);
            if (fromRelPath != null) {
                candidateNodes = fromRelPath.getMatchingPathMap(securityContext, new NodePathContext(this.actor, this.actor), null);
            } else {
                throw new RuntimeException("Error parsing '" + fromLimit + "' as path") ;
            }
        }

        if (schemaLimit != null) { // Filter by INSTANCEOF first, as it's quite a cheap operation
            if (schemaLimit.startsWith("\"") && schemaLimit.endsWith("\"")) {
                schemaLimit = schemaLimit.substring(1, schemaLimit.length() - 1);
            }
            List<AbsoluteNodePath> newCandidates = new ArrayList<>();
            for (AbsoluteNodePath candidate : candidateNodes) {
                if (schemaLimit.equals(candidate.getNode(securityContext).getSchema())) {
                    newCandidates.add(candidate);
                }
            }
            candidateNodes = newCandidates;
        }

        if (condition != null) {
            List<AbsoluteNodePath> newCandidates = new ArrayList<>();
            for (AbsoluteNodePath candidate : candidateNodes) {
                if (condition.eval(securityContext, new NodePathContext(this.actor, candidate.getNode(securityContext)))) {
                    newCandidates.add(candidate);
                }
            }
            candidateNodes = newCandidates;
        }

        for (AbsoluteNodePath candidate : candidateNodes) { // Expand the output to provide every node
            Map<String, Tuple<String, String>> resultRepresentation = new HashMap<>();
            for (String property : requestedProperties) {
                List<AbsoluteNodePath> matches = new RelativeNodePath(property).getMatchingPathMap(securityContext, new NodePathContext(this.actor, candidate), null);
                //resultRepresentation.put(property, matches);
                for (AbsoluteNodePath match : matches) {
                    String path = match.toString();
                    String[] propertyComponents = property.split("/");
                    String[] pathComponents = path.split("/");
                    for (int i = propertyComponents.length-1; i >= 0; i--) {
                        if (propertyComponents[i].equals("*") || propertyComponents[i].equals("#")) {
                            propertyComponents[i] = pathComponents[i + (pathComponents.length - propertyComponents.length)];
                        }
                    }
                    String bin = String.join("/", propertyComponents);
                    String value = match.eval(new SecurityContext(graphDatabase, this.actor));
                    resultRepresentation.put(bin, new Tuple<>(path, value));
                }
            }
            output.add(resultRepresentation);
        }


        return output;
    }

}
