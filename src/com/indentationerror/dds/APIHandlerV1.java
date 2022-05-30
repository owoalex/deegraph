package com.indentationerror.dds;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class APIHandlerV1 implements HttpHandler {
    private DatabaseInstance databaseInstance;

    public APIHandlerV1(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
    }

    private JSONObject nodeToJson(Node node) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        JSONObject response = new JSONObject();
        response.put("@id", node.getId());
        if (!node.getGlobalId().getInstanceId().equals(this.databaseInstance.getInstanceId())) {
            response.put("@oid", node.getGlobalId());
        }
        response.put("@ctime", df.format(node.getCTime()));
        if (!node.getCTime().equals(node.getOCTime())) {
            response.put("@octime", df.format(node.getOCTime()));
        }
        if (node.getCNode() == null) {
            response.put("@ocnode", node.getOCNodeId());
        }
        if (node.getCNode() != null) {
            response.put("@cnode", node.getCNode().getId());
        }
        response.put("@data", node.getData());
        response.put("@schema", node.getSchema());
        for (String key : node.getProperties().keySet()) {
            response.put(key, node.getProperty(key).getId());
        }

        return response;
    }
    @Override
    public void handle(HttpExchange t) throws IOException {
        HttpsExchange httpsExchange = (HttpsExchange) t;

        String request = t.getRequestURI().toString()
                .replaceAll("%40", "@")
                .replaceAll("%7B", "{")
                .replaceAll("%7D", "}")
                .replaceAll("%20", " ")
                .replaceAll("%5C", "\\")
                .replaceAll("%22", "\"")
                .replaceAll("%27", "\'");
        String[] urlComps = request.split("\\?");
        //String requestMethod = t.getRequestMethod();
        String[] requestPath = urlComps[0].split("/");
        requestPath = Arrays.copyOfRange(requestPath, 3, requestPath.length);

        JSONObject response = new JSONObject();
        int responseCode = 200;

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        try {
            Node userNode = this.databaseInstance.getNode(this.databaseInstance.getInstanceId()); // Do everything from the root node for now
            Node headNode = userNode;
            Node tailNode = headNode;
            boolean outputTailNode = false;

            if (requestPath.length > 0) {
                if (requestPath[0].startsWith("@")) {
                    switch (requestPath[0]) {
                        case "@auth":
                            response.put("@token", requestPath[1]);
                            break;
                        case "@query":
                            String[] reconstruct = Arrays.copyOfRange(requestPath, 1, requestPath.length);
                            String queryText = String.join("/", reconstruct);
                            Query query = new Query(queryText, userNode);
                            switch (query.getQueryType()) {
                                case GRANT:
                                    query.runGrantQuery(this.databaseInstance);
                                    break;
                                case SELECT:
                                    List<Map<String, Node>> results = query.runSelectQuery(this.databaseInstance);
                                    JSONArray outputArray = new JSONArray();
                                    for (Map<String, Node> result : results) {
                                        JSONObject outNode = new JSONObject();
                                        for (String key : result.keySet()) {
                                            Node node = result.get(key);
                                            if (node != null) {
                                                outNode.put(key, nodeToJson(node));
                                            } else {
                                                outNode.put(key, JSONObject.NULL);
                                            }
                                        }
                                        outputArray.put(outNode);
                                    }
                                    response.put("@rows", outputArray);
                                    break;
                            }
                            break;
                        case "@new":
                            Node newNode = this.databaseInstance.newNode("data:text/plain,TEST", userNode, null);
                            response.put("@id", newNode.getId());
                            break;
                        case "@server_info":
                            response.put("@instance_id", this.databaseInstance.getInstanceId().toString());
                            break;
                    }
                } else {
                    outputTailNode = true;
                    int range = requestPath.length;
                    switch (t.getRequestMethod()) {
                        case "POST":
                        case "PUT":
                            range--;
                            break;
                        case "GET":
                        case "DELETE":
                        default:
                            break;
                    }
                    String[] reconstruct = Arrays.copyOfRange(requestPath, 0, range);
                    tailNode = new AbsoluteNodePath(String.join("/", reconstruct)).getNodeFrom(this.databaseInstance);
                    //response.put("@request_path", String.join("/", reconstruct));
                }
            } else {
                outputTailNode = true;
            }

            if (outputTailNode) {
                if (tailNode == null) {
                    responseCode = 404;
                    response.put("@error", "NodeNotFound");
                } else {
                    response = nodeToJson(tailNode);
                }
            }
        } catch (Exception e) {
            response = new JSONObject();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            response.put("error", sw.toString().replaceAll("\t", "    ").split("\n"));
            responseCode = 500;
        }

        byte[] responseRaw = response.toString(4).getBytes();
        Headers responseHeaders = httpsExchange.getResponseHeaders();
        responseHeaders.add("Access-Control-Allow-Origin", "*");
        responseHeaders.add("Content-Type", "application/json");
        httpsExchange.sendResponseHeaders(responseCode, responseRaw.length);
        OutputStream os = httpsExchange.getResponseBody();
        os.write(responseRaw);
        os.close();
    }
}
