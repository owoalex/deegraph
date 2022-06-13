package com.indentationerror.dds.query;

import com.indentationerror.dds.conditions.Condition;
import com.indentationerror.dds.database.*;

import java.text.ParseException;
import java.util.*;

public class Query {
    private LinkedList<String> parsedQuery;
    private QueryType queryType;
    private Node actor;
    public Query(String src, Node actor) throws ParseException {
        StringBuilder inputWordBuffer = new StringBuilder();
        Stack<Character> inputString = new Stack<>();
        char[] inputArray = src.trim().toCharArray();
        for (int i = inputArray.length - 1; i >= 0; i--) {
            inputString.push(inputArray[i]);
        }
        parsedQuery = new LinkedList<>();
        boolean quoteEscaped = false;

        while (inputString.size() > 0) {
            char currentChar = inputString.pop();
            if (quoteEscaped) {
                if (currentChar == '\\') {
                    currentChar = inputString.pop();
                    inputWordBuffer.append(currentChar);
                } else if (currentChar == '"') {
                    inputWordBuffer.append("\"");
                    parsedQuery.add(inputWordBuffer.toString());
                    inputWordBuffer = new StringBuilder();
                    quoteEscaped = false;
                } else {
                    inputWordBuffer.append(currentChar);
                }
            } else {
                switch (currentChar) {
                    case '!':
                    case '=':
                    case '>':
                    case '<':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        StringBuilder infix = new StringBuilder();
                        infix.append(currentChar);
                        boolean cancel = false;
                        while (!cancel) {
                            switch (inputString.peek()) {
                                case '!':
                                case '=':
                                case '>':
                                case '<':
                                    infix.append(inputString.pop());
                                    break;
                                case ' ':
                                    inputString.pop();
                                    cancel = true;
                                    break;
                                default:
                                    cancel = true;
                            }
                        }
                        parsedQuery.add(infix.toString());
                        break;
                    case ',':
                    case ';':
                    case '(':
                    case ')':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        parsedQuery.add(String.valueOf(currentChar));
                        break;
                    case '"':
                        inputWordBuffer.append("\"");
                        quoteEscaped = true;
                        break;
                    case ' ':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        break;
                    case '\\':
                        currentChar = inputString.pop();
                        inputWordBuffer.append(currentChar);
                        break;
                    default:
                        inputWordBuffer.append(currentChar);
                }
            }
        }

        if (inputWordBuffer.length() > 0) {
            parsedQuery.add(inputWordBuffer.toString());
        }

        String qtype = parsedQuery.poll();
        switch (qtype.toUpperCase(Locale.ROOT)) {
            case "GRANT":
                this.queryType = QueryType.GRANT;
                break;
            case "SELECT":
                this.queryType = QueryType.SELECT;
                break;
            default:
                throw new ParseException("'" + qtype + "' is not a valid query type", 0);
        }
    }

    private Condition parseConditionFromRemaining(GraphDatabase graphDatabase) {
        LinkedList<String> conditionElements = new LinkedList<>();
        boolean escape = false;
        String current;
        int bracketLevel = 0;
        boolean allowLiteral = true;
        while (!escape) {
            current = parsedQuery.peek();
            if (current == null) {
                escape = true;
                continue;
            }
            switch (current.toUpperCase(Locale.ROOT)) {
                case "(":
                    if (allowLiteral) {
                        bracketLevel++;
                        escape = false;
                    } else {
                        escape = true;
                    }
                    break;
                case ")":
                    allowLiteral = false;
                    bracketLevel--;
                    break;
                case "==":
                case "!=":
                case "AND":
                case "OR":
                case "NOT":
                    allowLiteral = true;
                    break;
                default:
                    if (allowLiteral) {
                        allowLiteral = false;
                    } else {
                        escape = true;
                    }
            }
            if (!escape) {
                conditionElements.add(current);
                parsedQuery.poll();
            }
        }
        if (bracketLevel != 0) { // We should only ever consider the conditions to be over once we're at the same bracket level we started at - this is a syntax error!
            new ParseException("Parse error on WHILE condition", 0);
        }
        return Condition.fromComponents(graphDatabase, conditionElements);
    }
    public void runGrantQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
        if (this.queryType != QueryType.GRANT) {
            throw new NoSuchMethodException();
        }

        System.out.println("GRANT QUERY");
        ArrayList<AuthorizedAction> actions = new ArrayList<>();
        boolean escape = false;
        String current = null;
        while (!escape) {
            current = parsedQuery.poll();
            switch(current.toUpperCase(Locale.ROOT)) {
                case "WRITE":
                    actions.add(AuthorizedAction.WRITE);
                    break;
                case "READ":
                    actions.add(AuthorizedAction.READ);
                    break;
                case "DELETE":
                    actions.add(AuthorizedAction.DELETE);
                    break;
                case "DELEGATE":
                    actions.add(AuthorizedAction.DELEGATE);
                    break;
                case "GRANT":
                    actions.add(AuthorizedAction.GRANT);
                    break;
                default:
                    throw new ParseException("'" + current + "' is not a valid permission", 0);
            }
            current = parsedQuery.poll();
            escape = !(current.trim().equals(","));
        }
        NodePath onObject = null;
        Condition condition = null;
        escape = false;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "ON":
                    onObject = new RelativeNodePath(parsedQuery.poll());
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

        AuthorizationRule rule = new AuthorizationRule(condition, actions.toArray(new AuthorizedAction[0]));

        //condition.eval(new NodePathContext(this.actor, object));
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
                        candidateNodes = new ArrayList<>(Arrays.asList(fromRelPath.getMatchingNodes(new NodePathContext(actor, null), graphDatabase.getAllNodes())));
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
                Node[] matches = new RelativeNodePath(property).getMatchingNodes(new NodePathContext(actor, candidate), graphDatabase.getAllNodes());
                resultRepresentation.put(property, matches);
            }
            outputMaps.add(resultRepresentation);
        }
        return outputMaps;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void runOn(GraphDatabase graphDatabase) throws ParseException {
        try {
            switch (queryType) {
                case GRANT:
                    runGrantQuery(graphDatabase);
                    break;
                case SELECT:
                    runSelectQuery(graphDatabase);
                    break;
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
