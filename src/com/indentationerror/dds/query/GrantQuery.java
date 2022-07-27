package com.indentationerror.dds.query;

import com.indentationerror.dds.conditions.Condition;
import com.indentationerror.dds.database.*;

import java.text.ParseException;
import java.util.*;

public class GrantQuery extends Query {
    protected GrantQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public UUID runGrantQuery(GraphDatabase graphDatabase) throws ParseException, NoSuchMethodException {
        if (this.queryType != QueryType.GRANT) {
            throw new NoSuchMethodException();
        }

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
                case "ACT":
                    actions.add(AuthorizedAction.ACT);
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
                        onNodes = new ArrayList<>(Arrays.asList(fromRelPath.getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe())));
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
        List<AuthorizedAction> authorizedActions = Arrays.asList(graphDatabase.getPermsOnNode(this.actor, graphDatabase.getNodeUnsafe(graphDatabase.getInstanceId())));
        if (authorizedActions.contains(AuthorizedAction.ACT)) { // Only allow users who have permissions to act as the instance (root) node to write permissions
            if (onNodes == null) {
                graphDatabase.registerRule(rule, "*");
            } else {
                for (Node cn : onNodes) {
                    graphDatabase.registerRule(rule, cn.getId().toString());
                }
                graphDatabase.registerRule(rule, this.actor.getId().toString());
            }
        }
        return rule.getUuid();

        //condition.eval(new NodePathContext(this.actor, object));
    }
}
