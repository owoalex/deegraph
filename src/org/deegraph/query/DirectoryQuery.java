package org.deegraph.query;

import org.deegraph.database.*;

import java.text.ParseException;
import java.util.*;

public class DirectoryQuery extends Query {
    protected DirectoryQuery(String src, Node actor) throws ParseException {
        super(src, actor);
    }

    public Map<String, Node> runDirectoryQuery(GraphDatabase graphDatabase) throws NoSuchMethodException, QueryException {
        if (this.queryType != QueryType.DIRECTORY) {
            throw new NoSuchMethodException();
        }

        if (parsedQuery.size() == 0) {
            parsedQuery.offer(".");
        }

        Node[] valueNodes = new RelativeNodePath(parsedQuery.poll()).getMatchingNodes(new SecurityContext(graphDatabase, this.actor), new NodePathContext(this.actor, this.actor), null);
        Node valueNode = (valueNodes.length == 1) ? valueNodes[0] : null;

        if (valueNode == null) {
            throw new QueryException(QueryExceptionCode.MISSING_SUBJECT);
        }

        //return valueNode.getProperties(new SecurityContext(graphDatabase, this.actor));
        return valueNode.getPropertiesUnsafe();
    }
}
