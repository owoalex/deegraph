package org.deegraph.query;

import org.deegraph.database.*;

import java.text.ParseException;

public class DeleteQuery extends Query {
    protected DeleteQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public boolean runDeleteQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException {
        if (this.queryType != QueryType.DELETE) {
            throw new NoSuchMethodException();
        }

        Node[] valueNodes = new RelativeNodePath(parsedQuery.poll()).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, null), graphDatabase.getAllNodesUnsafe());

        boolean deleteFailed = false;

        for (Node node : valueNodes) {
            if (node.completeUnlink(new SecurityContext(graphDatabase, this.actor))) {
                graphDatabase.unregisterNodeUnsafe(node); // Complete unlink will only work with delete perms
            } else {
                deleteFailed = true;
            }
        }

        return !deleteFailed;
    }
}
