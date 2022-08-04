package org.deegraph.database;

import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbsoluteNodePath {

    String[] pathComponents;
    public AbsoluteNodePath(String path) {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        this.pathComponents = path.split("/");
    }

    /**
     * Returns the node the path refers to. If the path has a * selector, it will return null.
     * @param graphDatabase
     * @return The node if path is exact, else null
     */
    public Node getNodeFrom(GraphDatabase graphDatabase, SecurityContext securityContext) {
        Node tailNode = null;
        int i = 0;
        int pathTraversalLength = pathComponents.length;
        if (pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}")) {
            i = 1;
            Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(pathComponents[0]);
            if (matcher.find()) {
                String uuid = matcher.group();
                Node candidateTailNode = graphDatabase.getNodeUnsafe(UUID.fromString(uuid));
                if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), candidateTailNode)).contains(AuthorizedAction.READ)) {
                    tailNode = candidateTailNode;
                }
            } else {
                tailNode = null;
            }
        }
        if (tailNode != null) {
            while (i < pathTraversalLength) {
                if (pathComponents[i].length() > 0) {
                    if (tailNode != null) {
                        if (pathComponents[i].startsWith("@")) {
                            switch (pathComponents[i].toLowerCase(Locale.ROOT)) {
                                case "@creator":
                                    tailNode = tailNode.getCNode();
                                    break;
                            }
                        } else {
                            tailNode = tailNode.getProperty(securityContext, pathComponents[i]);
                        }
                    }
                }
                i++;
            }
        }
        return tailNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbsoluteNodePath)) return false;
        return Arrays.equals(pathComponents, ((AbsoluteNodePath) o).pathComponents);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(pathComponents);
    }

    @Override
    public String toString() {
        return String.join("/", pathComponents);
    }
}
