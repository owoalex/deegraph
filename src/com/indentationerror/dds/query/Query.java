package com.indentationerror.dds.query;

import com.indentationerror.dds.conditions.Condition;
import com.indentationerror.dds.database.*;
import com.indentationerror.dds.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.*;

public class Query {
    private LinkedList<String> parsedQuery;
    private QueryType queryType;
    private Node actor;
    public Query(String src, Node actor) throws ParseException {
        this.actor = actor;
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
            case "LINK":
                this.queryType = QueryType.LINK;
                break;
            case "DIRECTORY":
                this.queryType = QueryType.DIRECTORY;
                break;
            case "UNLINK":
                this.queryType = QueryType.UNLINK;
                break;
            case "CONSTRUCT":
                this.queryType = QueryType.CONSTRUCT;
                break;
            case "INSERT":
                this.queryType = QueryType.INSERT;
                break;
            case "DELETE":
                this.queryType = QueryType.DELETE;
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
    public UUID runGrantQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
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
        ArrayList<Node> onNodes = null;
        Condition condition = null;
        escape = false;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "ON":
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    if (fromRelPath != null) {
                        onNodes = new ArrayList<>(Arrays.asList(fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes())));
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path") ;
                    }
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
        if (onNodes == null) {
            List<AuthorizedAction> authorizedActions = Arrays.asList(graphDatabase.getPermsOnNode(this.actor, graphDatabase.getNode(graphDatabase.getInstanceId())));
            if (authorizedActions.contains(AuthorizedAction.GRANT)) { // Only allow users who have grant perms on the instance (root) node to write global permissions
                graphDatabase.registerRule(rule, "*");
            } else {
                return null; // Discard the rule - we didn't have permission to add it
            }
        } else {
            boolean atLeastOne = false;
            for (Node cn : onNodes) {
                List<AuthorizedAction> authorizedActions = Arrays.asList(graphDatabase.getPermsOnNode(this.actor, cn));
                if (authorizedActions.contains(AuthorizedAction.GRANT)) { // Check for each node to make sure the use rhas permissions
                    graphDatabase.registerRule(rule, cn.getId().toString());
                    atLeastOne = true;
                }
            }
            if (!atLeastOne) {
                return null; // Discard the rule - we didn't have permission to add it
            }
        }
        return rule.getUuid();

        //condition.eval(new NodePathContext(this.actor, object));
    }

    public boolean runLinkQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException {
        if (this.queryType != QueryType.LINK) {
            throw new NoSuchMethodException();
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        String linkName = "#"; // Wildcard for inserting at the next available numbered property
        Node[] valueNodes = new RelativeNodePath(current).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes());
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
                        Node[] toNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes());
                        toNode = (toNodes.length == 1) ? toNodes[0] : null;
                    } else {
                        throw new RuntimeException("Error parsing '" + fromStrPath + "' as path") ;
                    }
                    break;
                case "AS":
                    linkName = parsedQuery.poll();
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

        toNode.addProperty(new SecurityContext(graphDatabase, this.actor), linkName, valueNode);
        System.out.println("Attempting link");

        return true;
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
                        Node[] parentNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes());
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
            Node[] parentNodes = fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, parentNode), graphDatabase.getAllNodes());
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

    public Map<String, Node> runDirectoryQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException {
        if (this.queryType != QueryType.DIRECTORY) {
            throw new NoSuchMethodException();
        }

        Node[] valueNodes = new RelativeNodePath(parsedQuery.poll()).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodes());
        Node valueNode = (valueNodes.length == 1) ? valueNodes[0] : null;

        if (valueNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        //return valueNode.getProperties(new SecurityContext(graphDatabase, this.actor));
        return valueNode.getPropertiesUnsafe();
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

    public QueryType getQueryType() {
        return queryType;
    }

    public void runOn(GraphDatabase graphDatabase) throws QueryException {
        try {
            switch (queryType) {
                case GRANT:
                    runGrantQuery(graphDatabase);
                    break;
                case SELECT:
                    runSelectQuery(graphDatabase);
                    break;
                case LINK:
                    runLinkQuery(graphDatabase);
                    break;
                case DIRECTORY:
                    runDirectoryQuery(graphDatabase);
                    break;
                case UNLINK:
                    break;
                case DELETE:
                    break;
                case INSERT:
                    break;
                case CONSTRUCT:
                    break;
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unexpected code path taken");
        } catch (DuplicatePropertyException e) {
            throw new QueryException(QueryExceptionCode.DUPLICATE_PROPERTY_NAME);
        } catch (ParseException e) {
            throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD);
        }
    }
}
