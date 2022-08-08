package org.deegraph.database;

import org.deegraph.formats.Tuple;

import java.util.*;
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

    public HashMap<AbsoluteNodePath, Node> getMatchingNodeMap(SecurityContext securityContext, NodePathContext nodePathContext, Node[] searchSpace) {
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
                return new HashMap<>();
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
                return new HashMap<>();
            }
        }
        if (pathComponents[0].equals(".")) { // Special case, the user has explicitly asked for a contextual path
            if (objectId == null) {
                return new HashMap<>();
            }
            pathComponents[0] = "{" + objectId + "}";
        } else {
            if (!(pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}"))) { // Oh no, we're still not starting with a literal yet
                /* // Old way, kind of works how user expects but is less consistent - consistency is key!
                if (pathComponents[0].equals("*")) {
                    if (pathComponents.length == 1) { // Special case for ONLY the global operator - this can be used quite a few times in a single query, and should return quickly
                        //return searchSpace;
                        HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();
                        for (Node n: searchSpace) {
                            if (n != null) {
                                hmo.put(new AbsoluteNodePath("{" + n.getId() + "}"), n);
                            }
                        }
                        return hmo;
                    }
                } else { // Looks like this is an implicit contextual path, let'f fill in this for the user
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + 1);
                    newArray[0] = "{" + objectId + "}";
                    System.arraycopy(pathComponents, 0, newArray, 1, pathComponents.length);
                    pathComponents = newArray;
                }*/

                if (pathComponents[0].equals("**")) {
                    if (pathComponents.length == 1) { // Special case for ONLY the super global operator - this can be used quite a few times in a single query, and should return quickly
                        //return searchSpace;
                        HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();
                        for (Node n: searchSpace) {
                            if (n != null) {
                                hmo.put(new AbsoluteNodePath("{" + n.getId() + "}"), n);
                            }
                        }
                        return hmo;
                    }// else { // Otherwise, we just match basically anything
                    //    pathComponents = Arrays.copyOfRange(pathComponents, 1, pathComponents.length); // We do this by popping this operator off
                    //}
                } else { // Looks like this is an implicit contextual path, let'f fill in this for the user
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + 1);
                    newArray[0] = "{" + objectId + "}";
                    System.arraycopy(pathComponents, 0, newArray, 1, pathComponents.length);
                    pathComponents = newArray;
                }
            }
        }

        //System.out.println("EXPPATH: " + String.join("/", pathComponents));

        //ArrayList<Node> output = new ArrayList<>();
        HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();

        for (Node node : searchSpace) {
            List<Tuple<String, Node>> currentValidParents = new ArrayList<>();
            currentValidParents.add(new Tuple<>("", node));
            for (int i = pathComponents.length - 1; i >= 0; i--) { // We'll traverse backwards to see if things make sense - there must be at least one valid parent
                if (pathComponents[i].equals("*") || pathComponents[i].equals("#")) {
                    ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                    for (Tuple<String, Node> checkParent : currentValidParents) {
                        String tail = ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "");
                        for (String key : checkParent.y.getAllReferrersUnsafe().keySet()) {
                            if (pathComponents[i].equals("#")) {
                                if (key.matches("[0-9]+")) { // Numerical keys only!
                                    Node[] parentNodes = checkParent.y.getReferrers(securityContext, key);
                                    for (Node parentNode: parentNodes) {
                                        newValidParents.add(new Tuple<>( key + tail, parentNode));
                                    }
                                }
                            } else {
                                Node[] parentNodes = checkParent.y.getReferrers(securityContext, key);
                                for (Node parentNode: parentNodes) {
                                    newValidParents.add(new Tuple<>( key + tail, parentNode));
                                }
                            }
                        }
                    }
                    currentValidParents = newValidParents;
                } else if (pathComponents[i].equals("**")) {
                    ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                    for (Tuple<String, Node> checkParent : currentValidParents) {
                        String tail = ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "");
                        newValidParents.add(new Tuple<>( "{" + checkParent.y.getId() + "}" + tail, checkParent.y));
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
                        for (Tuple<String, Node> checkParent : currentValidParents) {
                            if (checkParent.y.getId().equals(uuid)) {
                                hmo.put(new AbsoluteNodePath("{" + checkParent.y.getId() + "}" + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "")), node);
                                //output.add(node); // This node is definitely valid, we've hit a parent with a defined uuid, we can stop looping through parents as anything above is irrelevant
                                i = 0; // Exit out of the for loop for this node
                            }
                        }
                        currentValidParents = new ArrayList<>(); // Special case needed to skip adding the node later
                    }
                } else if (pathComponents[i].startsWith("@")) { // Meta property - we only allow a few of these in traversals
                    ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                    switch (pathComponents[i].toLowerCase(Locale.ROOT)) {
                        case "@creator":
                            for (Tuple<String, Node> checkParent: currentValidParents) {
                                if (checkParent.y != null) {
                                    Node[] parentNodes = checkParent.y.getCreatedNodes(securityContext);
                                    System.out.println("Created " + parentNodes.length + " nodes");
                                    for (Node parentNode: parentNodes) {
                                        newValidParents.add(new Tuple<>( "@creator" + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
                                    }
                                }
                            }
                            break;
                    }
                    currentValidParents = newValidParents;
                } else {
                    ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                    for (Tuple<String, Node> checkParent : currentValidParents) {
                        if (checkParent.y != null) {
                            Node[] parentNodes = checkParent.y.getReferrers(securityContext, pathComponents[i]);
                            for (Node parentNode: parentNodes) {
                                newValidParents.add(new Tuple<>( pathComponents[i] + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
                            }
                        }
                    }
                    currentValidParents = newValidParents;
                }
                currentValidParents = currentValidParents.stream().distinct().collect(Collectors.toList());
            }
            for (Tuple<String, Node> checkParent : currentValidParents) {
                hmo.put(new AbsoluteNodePath(checkParent.x), node);
            }
            //if (currentValidParents.size() > 0) {
                //hmo.put(new AbsoluteNodePath("{" + node.getId() + "}"), node);
            //}
        }

        return hmo;
    }
    public Node[] getMatchingNodes(SecurityContext securityContext, NodePathContext nodePathContext, Node[] searchSpace) {
        HashMap<AbsoluteNodePath, Node> hmo = getMatchingNodeMap(securityContext, nodePathContext, searchSpace);
        return hmo.values().toArray(new Node[0]);
    }

    @Override
    public String toString() {
        return String.join("/", pathComponents);
    }
}
