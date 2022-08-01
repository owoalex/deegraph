package com.indentationerror.dds.tests;

import com.indentationerror.dds.database.*;
import com.indentationerror.dds.exceptions.DuplicatePropertyException;
import com.indentationerror.dds.exceptions.UnvalidatedJournalSegment;
import com.indentationerror.dds.query.GrantQuery;
import com.indentationerror.dds.query.Query;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicTests {
    protected static final String TEST_CONFIG_FILE = "./test.json";
    protected GraphDatabase initTestDb() throws UnvalidatedJournalSegment, IOException {
        File configFile = new File(TEST_CONFIG_FILE);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        JSONObject config = new JSONObject(jsonBuilder.toString());
        File dbDirectory = new File(config.getString("data_directory"));
        if (dbDirectory.exists()) {
            Files.walk(Paths.get(dbDirectory.getAbsolutePath()))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
        dbDirectory.mkdirs();

        return new GraphDatabase(TEST_CONFIG_FILE);
    }

    @Test
    public void writeAndRecall() throws UnvalidatedJournalSegment, IOException, DuplicatePropertyException, ParseException, NoSuchMethodException {
        GraphDatabase gdb = initTestDb();
        GrantQuery grantQuery = (GrantQuery) Query.fromString("GRANT READ,WRITE,DELETE,DELEGATE WHERE @creator_id == /@id", gdb.getInstanceNode());
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
    }
}
