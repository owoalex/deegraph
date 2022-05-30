package com.indentationerror.dds;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;
import org.json.JSONObject;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class APIHandlerV1 implements HttpHandler {
    private DatabaseInstance databaseInstance;

    public APIHandlerV1(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
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
            Node userNode = this.databaseInstance.newNode(null, null, null);
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
                            Query query = new Query(queryText);
                            query.runOn(this.databaseInstance);
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
                    response.put("@id", tailNode.getId());
                    response.put("@oid", tailNode.getGlobalId());
                    response.put("@ctime", df.format(tailNode.getCTime()));
                    response.put("@octime", df.format(tailNode.getOCTime()));
                    if (tailNode.getCNode() != null) {
                        response.put("@cnode", tailNode.getCNode().getId());
                    }
                    response.put("@ocnode", tailNode.getOCNodeId());
                    response.put("@data", tailNode.getData());
                    response.put("@schema", tailNode.getSchema());
                    for (String key : tailNode.getProperties().keySet()) {
                        response.put(key, tailNode.getProperty(key).getId());
                    }
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
