package org.deegraph.query;

import org.deegraph.conditions.Condition;
import org.deegraph.database.*;

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

        if (parsedQuery.size() == 0) {
            throw new ParseException("Empty GRANT query", 0);
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
                case "ACT":
                    actions.add(AuthorizedAction.ACT);
                    break;
                default:
                    throw new ParseException("'" + current + "' is not a valid permission", 0);
            }
            current = parsedQuery.poll();
            escape = !(current.trim().equals(","));
        }
        Condition condition = null;
        ArrayList<RelativeNodePath> validFor = new ArrayList<>();
        escape = false;
        boolean delegatable = false;
        while (!escape) {
            switch (current.toUpperCase(Locale.ROOT)) {
                case "ON":
                    String fromStrPath = parsedQuery.poll();
                    RelativeNodePath fromRelPath = new RelativeNodePath(fromStrPath);
                    validFor.add(fromRelPath);
                    break;
                case "WHERE":
                    condition = parseConditionFromRemaining(graphDatabase);
                    break;
                case "DELEGATABLE":
                    delegatable = true;
                    break;
                default:
                    throw new ParseException("'" + current + "' is not a valid control word", 0);
            }
            current = parsedQuery.poll();
            if (current == null || current.length() == 0) {
                escape = true;
            }
        }

        AuthorizationRule rule = new AuthorizationRule(validFor.toArray(new RelativeNodePath[0]), condition, actions.toArray(new AuthorizedAction[0]), delegatable);
        List<AuthorizedAction> authorizedActions = Arrays.asList(graphDatabase.getPermsOnNode(this.actor, graphDatabase.getNodeUnsafe(graphDatabase.getInstanceId())));
        if (authorizedActions.contains(AuthorizedAction.ACT)) { // Only allow users who have permissions to act as the instance (root) node to write permissions
            graphDatabase.registerRule(rule);
            return rule.getUuid();
        } else {
            return null;
        }

        //condition.eval(new NodePathContext(this.actor, object));
    }
}
