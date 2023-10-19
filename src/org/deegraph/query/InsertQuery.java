package org.deegraph.query;

import org.deegraph.database.*;
import org.deegraph.exceptions.ClosedJournalException;
import org.deegraph.exceptions.DuplicatePropertyException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Locale;

public class InsertQuery extends Query {
    protected InsertQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Node[] runInsertQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException, DuplicatePropertyException, ClosedJournalException, ParseException {
        if (this.queryType != QueryType.INSERT) {
            throw new NoSuchMethodException();
        }

        if (parsedQuery.size() == 0) {
            throw new ParseException("Empty INSERT query", 0);
        }

        boolean escape = false;
        String current = parsedQuery.poll();
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        ArrayList<String> schemas = new ArrayList<>();
        String into = null;
        boolean innerEscape = false;
        boolean duplicate = false;
        boolean replace = false;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "KEYS":
                    innerEscape = false;
                    while (!innerEscape) {
                        current = parsedQuery.poll();
                        keys.add(current);
                        current = parsedQuery.poll();
                        if (current == null) {
                            break;
                        }
                        innerEscape = !(current.trim().equals(","));
                    }
                    if (current != null) {
                        parsedQuery.addFirst(current);
                    }
                    break;
                case "SCHEMAS":
                    innerEscape = false;
                    while (!innerEscape) {
                        current = parsedQuery.poll();
                        if (current.startsWith("\"") && current.endsWith("\"")) {
                            current = current.substring(1, current.length() - 1);
                        } else {
                            if (current.toLowerCase(Locale.ROOT).equals("null")) {
                                current = null;
                            }
                        }
                        schemas.add(current);
                        current = parsedQuery.poll();
                        if (current == null) {
                            break;
                        }
                        innerEscape = !(current.trim().equals(","));
                    }
                    if (current != null) {
                        parsedQuery.addFirst(current);
                    }
                    break;
                case "VALUES":
                    innerEscape = false;
                    while (!innerEscape) {
                        current = parsedQuery.poll();
                        if (current.startsWith("\"") && current.endsWith("\"")) {
                            current = current.substring(1, current.length() - 1);
                        } else {
                            if (current.toLowerCase(Locale.ROOT).equals("null")) {
                                current = null;
                            }
                        }
                        values.add(current);
                        current = parsedQuery.poll();
                        if (current == null) {
                            break;
                        }
                        innerEscape = !(current.trim().equals(","));
                    }
                    if (current != null) {
                        parsedQuery.addFirst(current);
                    }
                    break;
                case "INTO":
                    into = parsedQuery.poll();
                    break;
                case "DUPLICATE":
                    duplicate = true;
                    break;
                case "REPLACE":
                    replace = true;
                    break;
                default:
                    throw new QueryException(QueryExceptionCode.INVALID_CONTROL_WORD, "'" + current + "' is not a valid control word");
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        boolean insertAsArray = false;
        boolean applySchemas = true;

        if (keys.size() == 0) {
            insertAsArray = true;
        } else {
            if (keys.size() != values.size()) {
                throw new QueryException(QueryExceptionCode.KEYS_DO_NOT_MAP_TO_VALUES);
            }
        }

        if (schemas.size() == 0) {
            applySchemas = false;
        } else {
            if (schemas.size() != values.size()) {
                throw new QueryException(QueryExceptionCode.SCHEMAS_DO_NOT_MAP_TO_VALUES);
            }
        }

        RelativeNodePath intoRelPath = new RelativeNodePath(into);
        Node[] nodes = intoRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, this.actor), null);
        ArrayList<Node> newNodes = new ArrayList<>();

        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            String key = "#";
            String schema = null;
            if (!insertAsArray) {
                key = keys.get(i);
            }
            if (applySchemas) {
                schema = schemas.get(i);
            }
            Node newNode = null;
            if (!duplicate) {
                newNode = graphDatabase.newNode(value, this.actor, schema);
                newNodes.add(newNode);
            }
            for (Node node: nodes) {
                if (duplicate) {
                    newNode = graphDatabase.newNode(value, this.actor, schema);
                    newNodes.add(newNode);
                }
                if ((!key.equals("#")) && node.hasProperty(new SecurityContext(graphDatabase, this.actor), key)) {
                    if (replace) {
                        node.removeProperty(new SecurityContext(graphDatabase, this.actor), key);
                        graphDatabase.getOpenJournal().registerEntry(new RemoveRelationJournalEntry(this.actor, node, key));
                        node.addProperty(new SecurityContext(graphDatabase, this.actor), key, newNode);
                        graphDatabase.getOpenJournal().registerEntry(new AddRelationJournalEntry(this.actor, node, key, newNode));
                    }
                } else {
                    node.addProperty(new SecurityContext(graphDatabase, this.actor), key, newNode);
                    graphDatabase.getOpenJournal().registerEntry(new AddRelationJournalEntry(this.actor, node, key, newNode));
                }
            }
        }


        //Node newNode = graphDatabase.newNode(data, this.actor, schema);

        //System.out.println(newNode.getCNode().getId());

        return newNodes.toArray(new Node[0]);
    }
}
