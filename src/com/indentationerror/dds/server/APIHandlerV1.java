package com.indentationerror.dds.server;

import com.indentationerror.dds.database.*;
import com.indentationerror.dds.query.Query;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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
            response.put("@global_id", node.getGlobalId());
        }
        response.put("@created", df.format(node.getCTime()));
        if (!node.getCTime().equals(node.getOCTime())) {
            response.put("@originally_created", df.format(node.getOCTime()));
        }
        if (node.getCNode() == null) {
            response.put("@original_creator", node.getOCNodeId());
        }
        if (node.getCNode() != null) {
            response.put("@creator", node.getCNode().getId());
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

        String request = URLDecoder.decode(t.getRequestURI().toString(), Charset.defaultCharset());
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

            String bodyContent = "";

            InputStreamReader isr = new InputStreamReader(t.getRequestBody(), Charset.defaultCharset());
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();
            while (line != null) {
                bodyContent = bodyContent + line + "\n";
                line = br.readLine();
            }
            //System.out.println("BC: " + bodyContent);

            if (requestPath.length > 0) {
                if (requestPath[0].startsWith("@")) {
                    switch (requestPath[0]) {
                        case "@resolve_path":
                            bodyContent = URLDecoder.decode(bodyContent.trim(), Charset.defaultCharset()).trim();
                            //System.out.println("pth: " + bodyContent);
                            tailNode = new RelativeNodePath(bodyContent).toAbsolute(new NodePathContext(userNode, userNode)).getNodeFrom(databaseInstance);
                            outputTailNode = true;
                            break;
                        case "@auth":
                            response.put("@token", requestPath[1]);
                            break;
                        case "@query":
                            String queryText = "";

                            if (t.getRequestMethod().toUpperCase(Locale.ROOT).equals("POST")) {
                                queryText = bodyContent;
                            } else {
                                String[] reconstruct = Arrays.copyOfRange(requestPath, 1, requestPath.length);
                                queryText = String.join("/", reconstruct);
                            }

                            Query query = new Query(queryText, userNode);
                            switch (query.getQueryType()) {
                                case GRANT:
                                    query.runGrantQuery(this.databaseInstance);
                                    break;
                                case SELECT:
                                    List<Map<String, Node[]>> results = query.runSelectQuery(this.databaseInstance);
                                    JSONArray outputArray = new JSONArray();
                                    for (Map<String, Node[]> result : results) {
                                        JSONObject outNode = new JSONObject();
                                        for (String key : result.keySet()) {
                                            JSONArray nodeList = new JSONArray();
                                            for (Node node : result.get(key)) {
                                                if (node != null) {
                                                    nodeList.put(nodeToJson(node));
                                                }
                                            }
                                            outNode.put(key, nodeList);
                                        }
                                        outputArray.put(outNode);
                                    }
                                    response.put("@rows", outputArray);
                                    break;
                            }
                            break;
                        case "@new":
                            Node newNode = this.databaseInstance.newNode(bodyContent.trim(), userNode, null);
                            response.put("@id", newNode.getId());
                            break;
                        case "@server_info":
                            response.put("@instance_id", this.databaseInstance.getInstanceId().toString());
                            break;
                        default:
                            response.put("@error", "EndpointNotFound");
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
                    tailNode = new RelativeNodePath(String.join("/", reconstruct)).toAbsolute(new NodePathContext(userNode, userNode)).getNodeFrom(databaseInstance);
                    //System.out.println(String.join("/", reconstruct));
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
