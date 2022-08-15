package org.deegraph.query;

import org.deegraph.conditions.Condition;
import org.deegraph.database.GraphDatabase;
import org.deegraph.database.Node;

import java.text.ParseException;
import java.util.*;

public class Query {
    protected LinkedList<String> parsedQuery;
    protected String originalTextQuery;
    protected QueryType queryType;
    protected Node actor;
    protected Query(String src, Node actor) throws ParseException {
        this.originalTextQuery = src;
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
            case "LN":
            case "LINK":
                this.queryType = QueryType.LINK;
                break;
            case "LS":
            case "DIR":
            case "DIRECTORY":
                this.queryType = QueryType.DIRECTORY;
                break;
            case "REFS":
            case "REFERENCES":
                this.queryType = QueryType.REFERENCES;
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
            case "DEL":
            case "DELETE":
                this.queryType = QueryType.DELETE;
                break;
            case "PERMS":
            case "PERMISSIONS":
                this.queryType = QueryType.PERMISSIONS;
                break;
            default:
                throw new ParseException("'" + qtype + "' is not a valid query type", 0);
        }
    }

    public static Query fromString(String query, Node actor) throws ParseException {
        String queryType = query.split(" ")[0].toUpperCase(Locale.ROOT);
        switch (queryType) {
            case "GRANT":
                return new GrantQuery(query, actor);
            case "SELECT":
                return new SelectQuery(query, actor);
            case "LN":
            case "LINK":
                return new LinkQuery(query, actor);
            case "LS":
            case "DIR":
            case "DIRECTORY":
                return new DirectoryQuery(query, actor);
            case "UNLINK":
                return new UnlinkQuery(query, actor);
            case "REFS":
            case "REFERENCES":
                return new ReferencesQuery(query, actor);
            case "PERMS":
            case "PERMISSIONS":
                return new PermissionsQuery(query, actor);
            case "CONSTRUCT":
                return new ConstructQuery(query, actor);
            case "INSERT":
                return new InsertQuery(query, actor);
            case "DEL":
            case "DELETE":
                return new DeleteQuery(query, actor);
            default:
                throw new ParseException("'" + queryType + "' is not a valid query type", 0);
        }
    }

    protected Condition parseConditionFromRemaining(GraphDatabase graphDatabase) {
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
                case "EQUALS":
                case "===":
                case "IS":
                case "!=":
                case "DIFFERENT":
                case "!==":
                case "ISNT":
                case "&&":
                case "AND":
                case "||":
                case "OR":
                case "^|":
                case "XOR":
                case "!":
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

    public QueryType getQueryType() {
        return queryType;
    }

    public String toString() {
        return this.originalTextQuery;
    }

    public Node getActor() {
        return actor;
    }
}
