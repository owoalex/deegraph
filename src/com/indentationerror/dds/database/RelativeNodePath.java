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

    public Node[] getMatchingNodes(NodePathContext nodePathContext, Node[] searchSpace) {
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
        }
        if (pathComponents[0].length() == 0) { // Equivalent of the path starting with "/"
            if (actorId == null) {
                return null;
            }
            pathComponents[0] = "{" + actorId + "}";
        }

        ArrayList<Node> output = new ArrayList<>();

        if (pathComponents.length == 1) {
            if (pathComponents[0].equals("*")) {
                return searchSpace;
            }
        } else if (pathComponents.length == 0) {
            return searchSpace;
        }

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
                                    newValidParents.addAll(Arrays.asList(checkParent.getReferrers(key)));
                                }
                            } else {
                                if (pathComponents[i].equals("*")) {
                                    newValidParents.addAll(Arrays.asList(checkParent.getReferrers(key)));
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
                        newValidParents.addAll(Arrays.asList(checkParent.getReferrers(pathComponents[i])));
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
