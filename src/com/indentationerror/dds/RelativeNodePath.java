package com.indentationerror.dds;

public class RelativeNodePath extends NodePath {
    String[] pathComponents;
    String pathString;
    NodePathContext nodePathContext;

    public RelativeNodePath(String path, NodePathContext nodePathContext) {
        this.nodePathContext = nodePathContext;
        this.pathComponents = path.split("/");
    }

    public AbsoluteNodePath toAbsolute() {
        if (pathComponents[0].equals(".")) { // Special case, the user has explicitly asked for a contextual path
            pathComponents[0] = "{" + this.nodePathContext.getObject().getId() + "}";
            return new AbsoluteNodePath(String.join("/", pathComponents));
        }
        if (pathComponents[0].startsWith("{") && pathComponents[0].endsWith("}")) {
            return new AbsoluteNodePath(this.pathString);
        }
        if (nodePathContext == null) {
            return null;
        }
        if (pathComponents[0].length() == 0) { // Equivalent of the path starting with "/"
            pathComponents[0] = "{" + this.nodePathContext.getActor().getId() + "}";
            return new AbsoluteNodePath(String.join("/", pathComponents));
        }
        return new AbsoluteNodePath("{" + this.nodePathContext.getObject().getId() + "}/" + String.join("/", pathComponents));
    }

    @Override
    public String toString() {
        return String.join("/", pathComponents);
    }
}
