package com.indentationerror.dds;

import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbsoluteNodePath {

    String[] pathComponents;
    public AbsoluteNodePath(String path) {
        this.pathComponents = path.split("/");
    }

    /**
     * Returns the node the path refers to. If the path has a * selector, it will return null.
     * @param databaseInstance
     * @return The node if path is exact, else null
     */
    public Node getNodeFrom(DatabaseInstance databaseInstance) {
        Node tailNode = null;
        int i = 0;
        int pathTraversalLength = pathComponents.length;
        if (pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}")) {
            i = 1;
            Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(pathComponents[0]);
            if (matcher.find()) {
                String uuid = matcher.group();
                tailNode = databaseInstance.getNode(UUID.fromString(uuid));
            } else {
                tailNode = null;
            }
        }
        if (tailNode != null) {
            while (i < pathTraversalLength) {
                if (pathComponents[i].length() > 0) {
                    if (tailNode != null) {
                        tailNode = tailNode.getProperty(pathComponents[i]);
                    }
                }
                i++;
            }
        }
        return tailNode;
    }

    public Node[] getMatchingNodes(DatabaseInstance databaseInstance, Node[] searchSpace) {
        ArrayList<Node> output = new ArrayList<>();

        for (Node node : searchSpace) {

        }

        return output.toArray(new Node[0]);
    }

    @Override
    public String toString() {
        return String.join("/", pathComponents);
    }
}
