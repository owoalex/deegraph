package org.deegraph.database;

import org.deegraph.formats.DataUrl;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class NodePath {
    public static String metaProp(GraphDatabase gdb, Node node, String key, Node requestingNode) throws ParseException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        switch (key) {
            case "@creator":
                if (node.getCNode() != null) {
                    String dataToParse = node.getCNode().getData(new SecurityContext(gdb, requestingNode));
                    if (dataToParse != null) {
                        return new DataUrl(dataToParse).getStringData();
                    }
                }
                break;
            case "@original_creator_id":
                return "{" + node.getOCNodeId().toString() + "}";
            case "@id":
                return "{" + node.getId().toString() + "}";
            case "@original_id":
                return "{" + node.getOriginalId().toString() + "}";
            case "@original_instance_id":
                return "{" + node.getOriginalInstanceId().toString() + "}";
            case "@created":
                return df.format(node.getCTime());
            case "@originally_created":
                return df.format(node.getOCTime());
            case "@data":
                String dataToParse = node.getData(new SecurityContext(gdb, requestingNode));
                if (dataToParse != null) {
                    return new DataUrl(dataToParse).getStringData();
                }
                break;
        }
        return null;
    }
    public static byte[] metaPropRaw(GraphDatabase gdb, Node node, String key, Node requestingNode) throws ParseException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        switch (key) {
            case "@creator":
                if (node.getCNode() != null) {
                    String data = node.getCNode().getData(new SecurityContext(gdb, requestingNode));
                    if (data != null) {
                        return data.getBytes(StandardCharsets.UTF_8);
                    }
                }
                break;
            case "@parsed_creator":
                if (node.getCNode() != null) {
                    String data = node.getCNode().getData(new SecurityContext(gdb, requestingNode));
                    if (data != null) {
                        return new DataUrl(data).getStringData().getBytes(StandardCharsets.UTF_8);
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
                String data = node.getData(new SecurityContext(gdb, requestingNode));
                if (data != null) {
                    return data.getBytes(StandardCharsets.UTF_8);
                }
                break;
            case "@parsed_data":
                String dataToParse = node.getData(new SecurityContext(gdb, requestingNode));
                if (dataToParse != null) {
                    return new DataUrl(dataToParse).getRawData();
                }
                break;
        }
        return null;
    }
}
