package org.deegraph.database;

import org.deegraph.formats.Tuple;

import java.text.ParseException;
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

    public List<AbsoluteNodePath> getMatchingPathMap(SecurityContext securityContext, NodePathContext nodePathContext, Node[] searchSpace) {
        String[] pathComponents = this.pathComponents.clone(); // Avoid side effects
        String objectPath = null;
        if (nodePathContext.getObject() != null) {
            objectPath = "{" + nodePathContext.getObject().getId().toString() + "}";
        }
        if (nodePathContext.getObjectPath() != null) {
            objectPath = nodePathContext.getObjectPath().toString();
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
                return new ArrayList<>();
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
                return new ArrayList<>();
            }
        }
        if (pathComponents[0].equals(".")) { // Special case, the user has explicitly asked for a contextual path
            if (objectPath == null) {
                return new ArrayList<>();
            }

            String[] objectPathComponents = objectPath.replaceAll("\\\\$|^\\\\", "").split("/");
            String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + objectPathComponents.length - 1);
            System.arraycopy(objectPathComponents, 0, newArray, 0, objectPathComponents.length);
            System.arraycopy(pathComponents, 1, newArray, objectPathComponents.length, pathComponents.length - 1);
            pathComponents = newArray;
        } else {
            if (!(pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}"))) { // Oh no, we're still not starting with a literal yet
                if (pathComponents[0].equals("**")) {
                    if (pathComponents.length == 1) { // Special case for ONLY the super global operator - this can be used quite a few times in a single query, and should return quickly
                        //return searchSpace;
                        ArrayList<AbsoluteNodePath> output = new ArrayList<>();
                        if (searchSpace == null) {
                            searchSpace = securityContext.getDatabase().getAllVisibleNodes(securityContext.getActor());
                        }
                        for (Node n : searchSpace) {
                            if (n != null) {
                                output.add(new AbsoluteNodePath("{" + n.getId() + "}"));
                            }
                        }
                        return output;
                    }
                } else { // Looks like this is an implicit contextual path, let's fill in this for the user
                    String[] objectPathComponents = objectPath.replaceAll("\\\\$|^\\\\", "").split("/");
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + objectPathComponents.length);
                    System.arraycopy(objectPathComponents, 0, newArray, 0, objectPathComponents.length);
                    System.arraycopy(pathComponents, 0, newArray, objectPathComponents.length, pathComponents.length);
                    pathComponents = newArray;
                }
            }
        }

        if (pathComponents[0].equals("**")) {
            ArrayList<AbsoluteNodePath> output = new ArrayList<>();

            if (searchSpace == null) {
                searchSpace = securityContext.getDatabase().getAllVisibleNodes(securityContext.getActor());
            }
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
                                        for (Node parentNode : parentNodes) {
                                            newValidParents.add(new Tuple<>(key + tail, parentNode));
                                        }
                                    }
                                } else {
                                    Node[] parentNodes = checkParent.y.getReferrers(securityContext, key);
                                    for (Node parentNode : parentNodes) {
                                        newValidParents.add(new Tuple<>(key + tail, parentNode));
                                    }
                                }
                            }
                        }
                        currentValidParents = newValidParents;
                    } else if (pathComponents[i].equals("**")) {
                        ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                        for (Tuple<String, Node> checkParent : currentValidParents) {
                            String tail = ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "");
                            newValidParents.add(new Tuple<>("{" + checkParent.y.getId() + "}" + tail, checkParent.y));
                        }
                        currentValidParents = newValidParents;
                    } else if (pathComponents[i].startsWith("{") && pathComponents[i].endsWith("}")) {
                        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
                        Matcher matcher = pattern.matcher(pathComponents[i]);
                        if (matcher.find()) {
                            UUID uuid = UUID.fromString(matcher.group());
                            for (Tuple<String, Node> checkParent : currentValidParents) {
                                if (checkParent.y.getId().equals(uuid)) {
                                    output.add(new AbsoluteNodePath("{" + checkParent.y.getId() + "}" + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "")));
                                    // This node is definitely valid, we've hit a parent with a defined uuid, we can stop looping through parents as anything above is irrelevant
                                    i = 0; // Exit out of the for loop for this node
                                }
                            }
                            currentValidParents = new ArrayList<>(); // Special case needed to skip adding the node later
                        }
                    } else if (pathComponents[i].startsWith("@")) { // Meta property - we only allow a few of these in traversals
                        ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                        switch (pathComponents[i].toLowerCase(Locale.ROOT)) {
                            case "@creator":
                                for (Tuple<String, Node> checkParent : currentValidParents) {
                                    if (checkParent.y != null) {
                                        Node[] parentNodes = checkParent.y.getCreatedNodes(securityContext);
                                        //System.out.println("Created " + parentNodes.length + " nodes");
                                        for (Node parentNode : parentNodes) {
                                            newValidParents.add(new Tuple<>("@creator" + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
                                        }
                                    }
                                }
                                break;
                            default:
                                for (Tuple<String, Node> checkParent : currentValidParents) {
                                    if (checkParent.y != null) {
                                        output.add(new AbsoluteNodePath(pathComponents[i].toLowerCase(Locale.ROOT) + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "")));
                                    }
                                }
                                i = 0; // Exit out of the for loop now, we've hit a meta property and can't go any further
                        }
                        currentValidParents = newValidParents;
                    } else {
                        ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                        for (Tuple<String, Node> checkParent : currentValidParents) {
                            if (checkParent.y != null) {
                                Node[] parentNodes = checkParent.y.getReferrers(securityContext, pathComponents[i]);
                                for (Node parentNode : parentNodes) {
                                    newValidParents.add(new Tuple<>(pathComponents[i] + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
                                }
                            }
                        }
                        currentValidParents = newValidParents;
                    }
                    currentValidParents = currentValidParents.stream().distinct().collect(Collectors.toList());
                }
                for (Tuple<String, Node> checkParent : currentValidParents) {
                    output.add(new AbsoluteNodePath(checkParent.x));
                }
            }

            return output;
        } else {
            List<Tuple<String, Node>> branches = new ArrayList<>();

            //System.out.println(String.join("/", pathComponents) + " AS " + securityContext.getActor().getId());
            boolean isRoot = securityContext.getDatabase().getInstanceNode().equals(securityContext.getActor());
            if (securityContext.getDatabase().getDebugSetting()) {
                if (!isRoot) {
                    System.out.println("RNP: " + String.join("/", pathComponents) + " AS " + securityContext.getActor().getId());
                }
            }
            Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(pathComponents[0]);
            if (matcher.find()) {
                UUID uuid = UUID.fromString(matcher.group());
                Node node = securityContext.getDatabase().getNode(uuid, securityContext.getActor());
                if (node != null) {
                    branches.add(new Tuple<>(pathComponents[0], node));
                }
            }
            List<Tuple<String, Node>> newBranches = new ArrayList<>();
            for (int level = 1; level < pathComponents.length; level++) {
                for (Tuple<String, Node> branch: branches) {
                    if (pathComponents[level].equals("*")) {
                        HashMap<String, Node> props = branch.y.getProperties(securityContext);
                        for (Map.Entry<String, Node> prop: props.entrySet()) {
                            newBranches.add(new Tuple<>(branch.x + "/" + prop.getKey(), prop.getValue()));
                        }
                    } else if (pathComponents[level].equals("#")) {
                        HashMap<String, Node> props = branch.y.getProperties(securityContext);
                        for (Map.Entry<String, Node> prop: props.entrySet()) {
                            if (prop.getKey().matches("[0-9]+")) {
                                newBranches.add(new Tuple<>(branch.x + "/" + prop.getKey(), prop.getValue()));
                            }
                        }
                    } else if (pathComponents[level].startsWith("@")) {
                        if (pathComponents[level].toLowerCase(Locale.ROOT).equals("@creator")) {
                            newBranches.add(new Tuple<>(branch.x + "/@creator", branch.y.getCNode()));
                        } else {
                            newBranches.add(new Tuple<>(branch.x + "/" + pathComponents[level].toLowerCase(Locale.ROOT), branch.y));
                            level = pathComponents.length; // Exit the loop now, this is a string property
                        }
                    } else {
                        Node node = branch.y.getProperty(securityContext, pathComponents[level]);
                        if (node != null) {
                            newBranches.add(new Tuple<>(branch.x + "/" + pathComponents[level], node));
                        }
                    }
                }
                branches = newBranches;
                newBranches = new ArrayList<>();
            }

            ArrayList<AbsoluteNodePath> output = new ArrayList<>();

            //HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();
            if (searchSpace == null) {
                for (Tuple<String, Node> branch : branches) {
                    output.add(new AbsoluteNodePath(branch.x));
                }
            } else {
                Collection<Node> searchSpaceCollection = Arrays.asList(searchSpace);
                for (Tuple<String, Node> branch : branches) {
                    for (Node node: searchSpaceCollection) {
                        if (node == branch.y) {
                            output.add(new AbsoluteNodePath(branch.x));
                        }
                    }
                }
            }
            //return hmo;

            return output;
        }
    }

    public HashMap<AbsoluteNodePath, Node> getMatchingNodeMap(SecurityContext securityContext, NodePathContext nodePathContext, Node[] searchSpace) {
        String[] pathComponents = this.pathComponents.clone(); // Avoid side effects
        String objectPath = null;
        if (nodePathContext.getObject() != null) {
            objectPath = "{" + nodePathContext.getObject().getId().toString() + "}";
        }
        if (nodePathContext.getObjectPath() != null) {
            objectPath = nodePathContext.getObjectPath().toString();
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
            if (objectPath == null) {
                return new HashMap<>();
            }
            //pathComponents[0] = objectPath;

            String[] objectPathComponents = objectPath.replaceAll("\\\\$|^\\\\", "").split("/");
            String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + objectPathComponents.length - 1);
            System.arraycopy(objectPathComponents, 0, newArray, 0, objectPathComponents.length);
            System.arraycopy(pathComponents, 1, newArray, objectPathComponents.length, pathComponents.length - 1);
            pathComponents = newArray;
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
                        if (searchSpace == null) {
                            searchSpace = securityContext.getDatabase().getAllVisibleNodes(securityContext.getActor());
                        }
                        for (Node n: searchSpace) {
                            if (n != null) {
                                hmo.put(new AbsoluteNodePath("{" + n.getId() + "}"), n);
                            }
                        }
                        return hmo;
                    }// else { // Otherwise, we just match basically anything
                    //    pathComponents = Arrays.copyOfRange(pathComponents, 1, pathComponents.length); // We do this by popping this operator off
                    //}
                } else { // Looks like this is an implicit contextual path, let's fill in this for the user
                    /*
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + 1);
                    newArray[0] = objectPath;
                    System.arraycopy(pathComponents, 0, newArray, 1, pathComponents.length);
                    pathComponents = newArray;
                     */

                    String[] objectPathComponents = objectPath.replaceAll("\\\\$|^\\\\", "").split("/");
                    String[] newArray = Arrays.copyOf(pathComponents, pathComponents.length + objectPathComponents.length);
                    System.arraycopy(objectPathComponents, 0, newArray, 0, objectPathComponents.length);
                    System.arraycopy(pathComponents, 0, newArray, objectPathComponents.length, pathComponents.length);
                    pathComponents = newArray;
                }
            }
        }

        //System.out.println("EXPPATH: " + String.join("/", pathComponents));

        if (pathComponents[0].equals("**")) {

            //System.out.println("EXPPATH: " + String.join("/", pathComponents));

            //ArrayList<Node> output = new ArrayList<>();
            HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();

            if (searchSpace == null) {
                searchSpace = securityContext.getDatabase().getAllVisibleNodes(securityContext.getActor());
            }
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
                                        for (Node parentNode : parentNodes) {
                                            newValidParents.add(new Tuple<>(key + tail, parentNode));
                                        }
                                    }
                                } else {
                                    Node[] parentNodes = checkParent.y.getReferrers(securityContext, key);
                                    for (Node parentNode : parentNodes) {
                                        newValidParents.add(new Tuple<>(key + tail, parentNode));
                                    }
                                }
                            }
                        }
                        currentValidParents = newValidParents;
                    } else if (pathComponents[i].equals("**")) {
                        ArrayList<Tuple<String, Node>> newValidParents = new ArrayList<>();
                        for (Tuple<String, Node> checkParent : currentValidParents) {
                            String tail = ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : "");
                            newValidParents.add(new Tuple<>("{" + checkParent.y.getId() + "}" + tail, checkParent.y));
                        }
                        currentValidParents = newValidParents;
                    } else if (pathComponents[i].startsWith("{") && pathComponents[i].endsWith("}")) {
                        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
                        Matcher matcher = pattern.matcher(pathComponents[i]);
                        if (matcher.find()) {
                            UUID uuid = UUID.fromString(matcher.group());
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
                                for (Tuple<String, Node> checkParent : currentValidParents) {
                                    if (checkParent.y != null) {
                                        Node[] parentNodes = checkParent.y.getCreatedNodes(securityContext);
                                        //System.out.println("Created " + parentNodes.length + " nodes");
                                        for (Node parentNode : parentNodes) {
                                            newValidParents.add(new Tuple<>("@creator" + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
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
                                for (Node parentNode : parentNodes) {
                                    newValidParents.add(new Tuple<>(pathComponents[i] + ((checkParent.x.length() > 0) ? ("/" + checkParent.x) : ""), parentNode));
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
            }

            return hmo;
        } else {
            List<Tuple<String, Node>> branches = new ArrayList<>();

            //System.out.println(String.join("/", pathComponents) + " AS " + securityContext.getActor().getId());
            boolean isRoot = securityContext.getDatabase().getInstanceNode().equals(securityContext.getActor());
            if (securityContext.getDatabase().getDebugSetting()) {
                if (!isRoot) {
                    System.out.println("RNP: " + String.join("/", pathComponents) + " AS " + securityContext.getActor().getId());
                }
            }
            Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(pathComponents[0]);
            if (matcher.find()) {
                UUID uuid = UUID.fromString(matcher.group());
                Node node = securityContext.getDatabase().getNode(uuid, securityContext.getActor());
                if (node != null) {
                    branches.add(new Tuple<>(pathComponents[0], node));
                }
            }
            List<Tuple<String, Node>> newBranches = new ArrayList<>();
            for (int level = 1; level < pathComponents.length; level++) {
                for (Tuple<String, Node> branch: branches) {
                    if (pathComponents[level].equals("*")) {
                        HashMap<String, Node> props = branch.y.getProperties(securityContext);
                        for (Map.Entry<String, Node> prop: props.entrySet()) {
                            newBranches.add(new Tuple<>(branch.x + "/" + prop.getKey(), prop.getValue()));
                        }
                    } else if (pathComponents[level].equals("#")) {
                        HashMap<String, Node> props = branch.y.getProperties(securityContext);
                        for (Map.Entry<String, Node> prop: props.entrySet()) {
                            if (prop.getKey().matches("[0-9]+")) {
                                newBranches.add(new Tuple<>(branch.x + "/" + prop.getKey(), prop.getValue()));
                            }
                        }
                    } else if (pathComponents[level].startsWith("@")) {
                        if (pathComponents[level].toLowerCase(Locale.ROOT).equals("@creator")) {
                            newBranches.add(new Tuple<>(branch.x + "/@creator", branch.y.getCNode()));
                        } else {
                            return new HashMap<>();
                        }
                    } else {
                        Node node = branch.y.getProperty(securityContext, pathComponents[level]);
                        if (node != null) {
                            newBranches.add(new Tuple<>(branch.x + "/" + pathComponents[level], node));
                        }
                    }
                }
                branches = newBranches;
                newBranches = new ArrayList<>();
            }

            HashMap<AbsoluteNodePath, Node> hmo = new HashMap<>();
            if (searchSpace == null) {
                for (Tuple<String, Node> branch : branches) {
                    hmo.put(new AbsoluteNodePath(branch.x), branch.y);
                }
            } else {
                Collection<Node> searchSpaceCollection = Arrays.asList(searchSpace);
                for (Tuple<String, Node> branch : branches) {
                    for (Node node: searchSpaceCollection) {
                        //System.out.println(branch.y.getId() + " == " + node.getId());
                        if (node == branch.y) {
                            hmo.put(new AbsoluteNodePath(branch.x), branch.y);
                        }
                    }
                }
            }
            return hmo;
        }
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
