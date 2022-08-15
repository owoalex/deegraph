package org.deegraph.conditions;

import org.deegraph.database.*;
import org.deegraph.formats.DataUrl;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;

public class EqualityCondition extends Condition {
    private Condition c1;
    private Condition c2;

    public EqualityCondition(GraphDatabase graphDatabase, Condition c1, Condition c2) {
        super(graphDatabase);
        this.c1 = c1;
        this.c2 = c2;
    }

    @Override
    public String toString() {
        return "(" + c1.toString() + " == " + c2.toString() + ")";
    }

    private byte[] metaProp(Node node, String key, NodePathContext context, Node requestingNode) throws ParseException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        switch (key) {
            case "@creator":
                if (node.getCNode() != null) {
                    if (node.getCNode().getDataUnsafe() != null) {
                        return new DataUrl(node.getCNode().getDataUnsafe()).getRawData();
                    }
                }
                break;
            case "@creator_id":
                if (node.getCNode() != null) {
                    if (node.getCNode().getId() != null) {
                        return ("{" + node.getCNode().getId().toString() + "}").getBytes(StandardCharsets.UTF_8);
                    }
                }
                break;
            case "@original_creator_id":
                return ("{" + node.getOCNodeId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@id":
                return ("{" + node.getId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@original_id":
                return ("{" + node.getOriginalId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@original_instance_id":
                return ("{" + node.getOriginalInstanceId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@created":
                return df.format(node.getCTime()).getBytes(StandardCharsets.UTF_8);
            case "@originally_created":
                return df.format(node.getOCTime()).getBytes(StandardCharsets.UTF_8);
            case "@data":
                if (node.getDataUnsafe() != null) {
                    return node.getData(new SecurityContext(this.graphDatabase, requestingNode)).getBytes(StandardCharsets.UTF_8);
                }
                break;
            case "@parsed_data":
                if (node.getDataUnsafe() != null) {
                    return new DataUrl(node.getData(new SecurityContext(this.graphDatabase, requestingNode))).getRawData();
                }
                break;
        }
        return null;
    }
    @Override
    public boolean eval(SecurityContext securityContext, NodePathContext nodePathContext) {
        try {
            String e1 = this.c1.asLiteral(securityContext,nodePathContext);
            String e2 = this.c2.asLiteral(securityContext,nodePathContext);

            Node requestingNode = nodePathContext.getActor();
            if (nodePathContext.getRequestingNode() != null) {
                requestingNode = nodePathContext.getRequestingNode();
            }
            //System.out.println(e1 + " == " + e2);

            byte[] rawValue1 = null;
            byte[] rawValue2 = null;

            if (e1.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue1 = e1.substring(1, e1.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                boolean absolute = e1.startsWith("/");
                String[] components = e1.split("/");
                String prop = "@data";
                if (components[components.length - 1].startsWith("@")) {
                    prop = components[components.length - 1];
                    e1 = String.join("/", Arrays.copyOfRange(components, 0, components.length - 1));
                    if (!absolute && e1.length() == 0) {
                        e1 = "."; // Special case, empty strings resulting from removing an @ property should be changed to a cd operator
                    }
                }
                if (absolute) {
                    e1 = "/" + e1;
                }
                Node e1Node = new RelativeNodePath(e1).toAbsolute(nodePathContext).getNodeFrom(this.graphDatabase, securityContext);
                if (e1Node != null) {
                    rawValue1 = metaProp(e1Node, prop, nodePathContext, requestingNode);
                }
            }


            if (e2.startsWith("\"")) { // Decide whether to treat as literal or not
                rawValue2 = e2.substring(1, e2.length() - 1).getBytes(StandardCharsets.UTF_8);
            } else {
                boolean absolute = e2.startsWith("/");
                String[] components = e2.split("/");
                String prop = "@data";
                if (components[components.length - 1].startsWith("@")) {
                    prop = components[components.length - 1];
                    e2 = String.join("/", Arrays.copyOfRange(components, 0, components.length - 1));
                    if (!absolute && e2.length() == 0) {
                        e2 = "."; // Special case, empty strings resulting from removing an @ property should be changed to a cd operator
                    }
                }
                if (absolute) {
                    e2 = "/" + e2;
                }
                Node e2Node = new RelativeNodePath(e2).toAbsolute(nodePathContext).getNodeFrom(this.graphDatabase, securityContext);
                if (e2Node != null) {
                    rawValue2 = metaProp(e2Node, prop, nodePathContext, requestingNode);
                }
            }


            if (rawValue1 == null || rawValue2 == null) {
                return (rawValue1 == null && rawValue2 == null);
            }
            //System.out.println(new String(rawValue1) + " == " + new String(rawValue2) + " : " + (Arrays.equals(rawValue1, rawValue2) ? "TRUE" : "FALSE"));
            return Arrays.equals(rawValue1, rawValue2);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
