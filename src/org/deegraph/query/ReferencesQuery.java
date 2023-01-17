package org.deegraph.query;

import org.deegraph.database.*;

import java.text.ParseException;
import java.util.Map;

public class ReferencesQuery extends Query {
    protected ReferencesQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Map<String, Node[]> runReferencesQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException {
        if (this.queryType != QueryType.REFERENCES) {
            throw new NoSuchMethodException();
        }

        Node[] valueNodes = new RelativeNodePath(parsedQuery.poll()).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor), graphDatabase.getAllNodesUnsafe());
        Node valueNode = (valueNodes.length == 1) ? valueNodes[0] : null;

        if (valueNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        return valueNode.getAllReferrers(new SecurityContext(graphDatabase, this.actor));
    }
}
