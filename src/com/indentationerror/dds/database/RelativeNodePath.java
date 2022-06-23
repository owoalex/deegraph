package com.indentationerror.dds.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RelativeNodePath extends NodePath {
    String[] pathComponents;
    String pathString;

    public RelativeNodePath(String path) {
        this.pathString = path;
        this.pathComponents = path.split("/");
    }

    public AbsoluteNodePath toAbsolute(NodePathContext nodePathContext) {
        if (this.pathString.startsWith("/")) { // Special case, this is an absolute path
            return new AbsoluteNodePath("{" + nodePathContext.getActor().getId() + "}" + this.pathString);
        }
        String[] pathComponents = this.pathComponents.clone(); // Avoid side effects
        String objectId = null;
        if (nodePathContext.getObject() != null) {
            objectId = nodePathContext.getObject().getId().toString();
        }
        String actorId = null;
        if (nodePathContext.getActor() != null) {
            actorId = nodePathContext.getActor().getId().toString();
        }
        if (pathComponents[0].equals(".")) { // Special case, the user has explicitly asked for a contextual path
            if (objectId == null) {
                return null;
            }
            pathComponents[0] = "{" + objectId + "}";
            return new AbsoluteNodePath(String.join("/", pathComponents));
        }
        if (pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}")) {
            return new AbsoluteNodePath(this.pathString);
        }
        if (nodePathContext == null) {
            return null;
        }
        if (pathComponents[0].length() == 0) { // Equivalent of the path starting with "/"
            if (actorId == null) {
                return null;
            }
            pathComponents[0] = "{" + actorId + "}";
            return new AbsoluteNodePath(String.join("/", pathComponents));
        }
        if (objectId == null) {
            return null;
        }
        return new AbsoluteNodePath("{" + objectId + "}/" + String.join("/", pathComponents));
    }

    public Node[] getMatchingNodes(SecurityContext securityContext, NodePathContext nodePathContext, Node[] searchSpace) {
        String[] pathComponents = this.pathComponents.clone(); // Avoid side effects
        String objectId = null;
        if (nodePathContext.getObject() != null) {
            objectId = nodePathContext.getObject().getId().toString();
        }
        String actorId = null;
        if (nodePathContext.getActor() != null) {
            actorId = nodePathContext.getActor().getId().toString();
        }
        if (this.pathString.startsWith("/")) {
            if (pathComponents.length == 0) {
                pathComponents = new String[1];
                pathComponents[0] = "";
            }
            if (actorId == null) {
                return new Node[0];
            }
            if (pathComponents[0].length() == 0) {
                pathComponents[0] = "{" + actorId + "}";
            } else {
                String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + 1);
                newArray[0] = "{" + actorId + "}";
                System.arraycopy(pathComponents, 0, newArray, 1, pathComponents.length);
                pathComponents = newArray;
            }
        } else {
            if (pathComponents.length == 0) {
                return new Node[0];
            }
        }
        if (pathComponents[0].equals(".")) { // Special case, the user has explicitly asked for a contextual path
            if (objectId == null) {
                return new Node[0];
            }
            pathComponents[0] = "{" + objectId + "}";
        } else {
            if (!(pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}"))) { // Oh no, we're still not starting with a literal yet
                if (pathComponents[0].equals("*")) {
                    if (pathComponents.length == 1) { // Special case for ONLY the global operator - this can be used quite a few times in a single query, and should return quickly
                        return searchSpace;
                    } // Otherwise we're fine - we know how to deal with this, this just means it can be the child of anything
                } else { // Looks like this is an implicit contextual path, let'f fill in this for the user
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + 1);
                    newArray[0] = "{" + objectId + "}";
                    System.arraycopy(pathComponents, 0, newArray, 1, pathComponents.length);
                    pathComponents = newArray;
                }
            }
        }

        //System.out.println("EXPPATH: " + String.join("/", pathComponents));

        ArrayList<Node> output = new ArrayList<>();

        for (Node node : searchSpace) {
            List<Node> currentValidParents = new ArrayList<>();
            currentValidParents.add(node);
            for (int i = pathComponents.length - 1; i >= 0; i--) { // We'll traverse backwards to see if things make sense - there must be at least one valid parent
                if (pathComponents[i].equals("*") || pathComponents[i].equals("#")) {
                    ArrayList<Node> newValidParents = new ArrayList<>();
                    for (Node checkParent : currentValidParents) {
                        for (String key : checkParent.getAllReferrers().keySet()) {
                            if (key.matches("[0-9].*")) {
                                if (pathComponents[i].equals("#")) {
                                    newValidParents.addAll(Arrays.asList(checkParent.getReferrers(securityContext, key)));
                                }
                            } else {
                                if (pathComponents[i].equals("*")) {
                                    newValidParents.addAll(Arrays.asList(checkParent.getReferrers(securityContext, key)));
                                }
                            }
                        }
                    }
                    currentValidParents = newValidParents;
                } else if (pathComponents[i].startsWith("{") && pathComponents[i].endsWith("}")) {
                    Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(pathComponents[i]);
                    if (matcher.find()) {
                        UUID uuid = UUID.fromString(matcher.group());
                        /*
                        ArrayList<Node> newValidParents = new ArrayList<>();
                        for (Node checkParent : currentValidParents) {
                            for (String key : checkParent.getAllReferrers().keySet()) {
                                for (Node referrer : checkParent.getReferrers(key)) {
                                    if (referrer.getId().equals(uuid)) {
                                        newValidParents.add(referrer);
                                    }
                                }
                            }
                        }*/
                        for (Node checkParent : currentValidParents) {
                            if (checkParent.getId().equals(uuid)) {
                                output.add(node); // This node is definitely valid, we've hit a parent with a defined uuid, we can stop looping through parents as anything above is irrelevant
                                i = 0; // Exit out of the for loop for this node
                            }
                        }
                        currentValidParents = new ArrayList<>(); // Special case needed to skip adding the node later
                    }
                } else {
                    ArrayList<Node> newValidParents = new ArrayList<>();
                    for (Node checkParent : currentValidParents) {
                        if (checkParent != null) {
                            newValidParents.addAll(Arrays.asList(checkParent.getReferrers(securityContext, pathComponents[i])));
                        }
                    }
                    currentValidParents = newValidParents;
                }
                currentValidParents = currentValidParents.stream().distinct().collect(Collectors.toList());
            }
            if (currentValidParents.size() > 0) {
                output.add(node);
            }
        }

        return output.toArray(new Node[0]);
    }

    @Override
    public String toString() {
        return String.join("/", pathComponents);
    }
}
