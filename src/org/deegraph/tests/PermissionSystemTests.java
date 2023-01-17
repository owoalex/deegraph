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

public class PermissionSystemTests {

    private GraphDatabase gdb;

    @Test
    public void runTests() throws UnvalidatedJournalSegment, IOException, DuplicatePropertyException, ParseException, NoSuchMethodException {
        gdb = TestUtilities.initTestDb();

        GrantQuery grantQuery = (GrantQuery) Query.fromString("GRANT READ,WRITE,DELETE WHERE @creator === /", gdb.getInstanceNode());
        grantQuery.runGrantQuery(gdb);



        Node originalCreator = gdb.newNode(null, gdb.getInstanceNode(), null);
        Node nodeB = gdb.newNode("food", originalCreator, null);
        Node nodeC = gdb.newNode("beans", originalCreator, null);
        Node nodeD = gdb.newNode("toast", originalCreator, null);
        nodeB.addProperty(new SecurityContext(gdb, originalCreator), "topping" ,nodeC);
        nodeB.addProperty(new SecurityContext(gdb, originalCreator), "substance" ,nodeD);

        RelativeNodePath rnp = new RelativeNodePath("{" + nodeB.getId() + "}/topping");
        Node[] allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, originalCreator), new NodePathContext(originalCreator), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, originalCreator)), "beans");
        }

        rnp = new RelativeNodePath("{" + nodeB.getId() + "}/substance");
        allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, originalCreator), new NodePathContext(originalCreator), gdb.getAllNodesUnsafe());
        for (Node node: allNodes) {
            assertEquals(node.getData(new SecurityContext(gdb, originalCreator)), "toast");
        }

        Node otherActor = gdb.newNode(null, gdb.getInstanceNode(), null);

        rnp = new RelativeNodePath("{" + nodeB.getId() + "}/substance");
        allNodes = rnp.getMatchingNodes(new SecurityContext(gdb, otherActor), new NodePathContext(otherActor), gdb.getAllNodesUnsafe());
        assertEquals(allNodes.length, 0);
    }
}
