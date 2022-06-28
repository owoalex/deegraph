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
                System.out.println("GRANT FAILED FOR * AS {" + this.actor.getId() + "}");
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
                System.out.println("GRANT FAILED FOR NODES");
                return null; // Discard the rule - we didn't have permission to add it
            }
        }
        return rule.getUuid();

        //condition.eval(new NodePathContext(this.actor, object));
    }
}
