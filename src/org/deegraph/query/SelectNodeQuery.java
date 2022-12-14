package org.deegraph.query;

import org.deegraph.conditions.Condition;
import org.deegraph.database.*;
import org.deegraph.formats.Tuple;
import org.deegraph.formats.TypeCoercionUtilities;

import java.text.ParseException;
import java.util.*;

import static org.deegraph.database.NodePath.metaProp;

public class SelectNodeQuery extends Query {
    protected SelectNodeQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Tuple<Map<UUID, Map<String, Map<AbsoluteNodePath, Node>>>, List<UUID>> runSelectNodeQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
        if (this.queryType != QueryType.SELECT_NODE) {
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
        String orderBy = null;
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
                Map<AbsoluteNodePath, Node> matches = new RelativeNodePath(property).getMatchingNodeMap(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, candidate), null);
                resultRepresentation.put(property, matches);
            }
            outputMaps.put(candidate.getId(), resultRepresentation);
        }

        List<UUID> order = null;

        if (orderBy != null) {
            boolean absolute = orderBy.startsWith("/");
            String[] components = orderBy.split("/");
            String prop = "@data";
            if (components[components.length - 1].startsWith("@")) {
                prop = components[components.length - 1];
                orderBy = String.join("/", Arrays.copyOfRange(components, 0, components.length - 1));
                if (!absolute && orderBy.length() == 0) {
                    orderBy = "."; // Special case, empty strings resulting from removing an @ property should be changed to a cd operator
                }
            }
            if (absolute) {
                orderBy = "/" + orderBy;
            }

            RelativeNodePath orderByPath = new RelativeNodePath(orderBy);

            order = new ArrayList<>();
            if (orderByPath != null) {
                TreeMap<Double, List<UUID>> priorityMap = new TreeMap<>();
                SecurityContext securityContext = new SecurityContext(graphDatabase, this.actor);
                for (Node candidate : candidateNodes) { // Expand the output to provide every node
                    double priority = 0;
                    Node[] matchingNodes = orderByPath.getMatchingNodes(securityContext, new NodePathContext(this.actor, candidate), null);
                    if (matchingNodes.length > 0) {
                        //matchingNodes[0]
                        try {
                            priority = TypeCoercionUtilities.coerceToNumber(metaProp(graphDatabase, matchingNodes[0], prop, this.actor));
                        } catch (NumberFormatException e) {
                            priority = 0;
                        }
                    }
                    if (!priorityMap.containsKey(priority)) {
                        priorityMap.put(priority, new ArrayList<>());
                    }
                    priorityMap.get(priority).add(candidate.getId());
                }
                SortedSet<Double> keys = new TreeSet<>(priorityMap.keySet());
                for (Double priority : keys) {
                    order.addAll(priorityMap.get(priority));
                }
            } else {
                throw new RuntimeException("Error parsing '" + orderBy + "' as path") ;
            }
        }

        return new Tuple<>(outputMaps, order);
    }

}
