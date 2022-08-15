package org.deegraph.tests;

import org.deegraph.database.*;
import org.deegraph.exceptions.DuplicatePropertyException;
import org.deegraph.exceptions.UnvalidatedJournalSegment;
import org.deegraph.query.GrantQuery;
import org.deegraph.query.Query;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicTests {


    @Test
    public void writeAndRecall() throws UnvalidatedJournalSegment, IOException, DuplicatePropertyException, ParseException, NoSuchMethodException {
        GraphDatabase gdb = TestUtilities.initTestDb();
        GrantQuery grantQuery = (GrantQuery) Query.fromString("GRANT READ,WRITE,DELETE WHERE @creator_id == /@id", gdb.getInstanceNode());
        grantQuery.runGrantQuery(gdb);
        Node actor = gdb.newNode(null, gdb.getInstanceNode(), null);
        Node nodeB = gdb.newNode("food", actor, null);
        Node nodeC = gdb.newNode("beans", actor, null);
        Node nodeD = gdb.newNode("toast", actor, null);
        nodeB.addProperty(new SecurityContext(gdb, actor), "topping" ,nodeC);
        nodeB.addProperty(new SecurityContext(gdb, actor), "substance" ,nodeD);

        RelativeNodePath rnp = new RelativeNodePath("{" + nodeB.getId() + "}/topping");
        Node[] allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, actor), new NodePathContext(actor, null), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, actor)), "beans");
        }

        rnp = new RelativeNodePath("{" + nodeB.getId() + "}/substance");
        allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, actor), new NodePathContext(actor, null), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, actor)), "toast");
        }

        gdb.shutdown();

        gdb = TestUtilities.reloadTestDb();

        rnp = new RelativeNodePath("{" + nodeB.getId() + "}/topping");
        allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, actor), new NodePathContext(actor, null), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, actor)), "beans");
        }

        rnp = new RelativeNodePath("{" + nodeB.getId() + "}/substance");
        allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, actor), new NodePathContext(actor, null), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, actor)), "toast");
        }
    }
}
