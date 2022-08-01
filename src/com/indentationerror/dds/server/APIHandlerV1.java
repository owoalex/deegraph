package com.indentationerror.dds.server;

import com.indentationerror.dds.database.*;
import com.indentationerror.dds.query.*;
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
    private GraphDatabase graphDatabase;

    public APIHandlerV1(GraphDatabase graphDatabase) {
        this.graphDatabase = graphDatabase;
    }

    private JSONObject nodeToJson(SecurityContext securityContext, Node node) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        JSONObject response = new JSONObject();
        if (Arrays.asList(securityContext.getDatabase().getPermsOnNode(securityContext.getActor(), node)).contains(AuthorizedAction.READ)) {
            response.put("@id", node.getId());
            if (!node.getOriginalInstanceId().equals(this.graphDatabase.getInstanceId())) {
                response.put("@original_instance_id", node.getOriginalInstanceId());
                response.put("@original_id", node.getOriginalId());
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
            response.put("@data", node.getData(securityContext));
            response.put("@schema", node.getSchema());
            for (String key : node.getProperties(securityContext).keySet()) {
                response.put(key, node.getProperty(securityContext, key).getId());
            }
            return response;
        } else {
            return null;
        }
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
            String authMethod = httpsExchange.getRequestHeaders().getFirst("Authorization");
            String loginNodeId = httpsExchange.getRequestHeaders().getFirst("X-Auxilium-Actor");
            Node userNode = this.graphDatabase.getNodeUnsafe(UUID.fromString(loginNodeId)); // Do everything from the root node for now
            SecurityContext securityContext = null;

            if (authMethod != null && authMethod.startsWith("Bearer ")) {
                String token = authMethod.substring("Bearer ".length());
                for (AuthenticationMethod validAuth : this.graphDatabase.getAuthMethods(userNode)) {
                    if (validAuth.validate(token)) {
                        securityContext = new SecurityContext(this.graphDatabase, userNode);
                    }
                }
            }

            if (securityContext == null) {
                throw new RuntimeException("Authentication failure");
            }

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
                            tailNode = new RelativeNodePath(bodyContent).toAbsolute(new NodePathContext(userNode, userNode)).getNodeFrom(graphDatabase, securityContext);
                            outputTailNode = true;
                            break;
                        case "@auth":
                            response.put("@token", requestPath[1]);
                            break;
                        case "@query":
                            if (userNode == null) {
                                break;
                            }
                            try {
                                String queryText = "";

                                if (t.getRequestMethod().toUpperCase(Locale.ROOT).equals("POST")) {
                                    queryText = bodyContent;
                                } else {
                                    String[] reconstruct = Arrays.copyOfRange(requestPath, 1, requestPath.length);
                                    queryText = String.join("/", reconstruct);
                                }

                                Query query = Query.fromString(queryText, userNode);

                                //System.out.println(query.getQueryType());

                                switch (query.getQueryType()) {
                                    case GRANT: {
                                        UUID ruleId = ((GrantQuery) query).runGrantQuery(this.graphDatabase);
                                        if (ruleId == null) {
                                            response.put("@error", "MissingGrantPermission");
                                            responseCode = 403;
                                        } else {
                                            response.put("@rule_id", ruleId);
                                            this.graphDatabase.recordQuery(query);
                                        }
                                        break;
                                    }
                                    case SELECT: {
                                        List<Map<String, Node[]>> results = ((SelectQuery) query).runSelectQuery(this.graphDatabase);
                                        JSONArray outputArray = new JSONArray();
                                        for (Map<String, Node[]> result : results) {
                                            JSONObject outNode = new JSONObject();
                                            for (String key : result.keySet()) {
                                                boolean atLeastOneValue = false;
                                                JSONArray nodeList = new JSONArray();
                                                for (Node node : result.get(key)) {
                                                    if (node != null) {
                                                        JSONObject nodeJsonRepr = nodeToJson(securityContext, node);
                                                        if (nodeJsonRepr != null) {
                                                            nodeList.put(nodeJsonRepr);
                                                            atLeastOneValue = true;
                                                        }
                                                    }
                                                }
                                                if (atLeastOneValue) { // If it's all null, just discard this row, it's not exactly helpful
                                                    outNode.put(key, nodeList);
                                                }
                                            }
                                            if (outNode.keySet().size() != 0) { // It's not very useful to add an empty output
                                                outputArray.put(outNode);
                                            }
                                        }
                                        response.put("@rows", outputArray);
                                        break;
                                    }
                                    case LINK: {
                                        if (((LinkQuery) query).runLinkQuery(this.graphDatabase)) {
                                            response.put("@response", "OK");
                                            this.graphDatabase.recordQuery(query);
                                        } else {
                                            response.put("@error", "FailedToLink");
                                        }
                                        break;
                                    }
                                    case UNLINK: {
                                        if (((UnlinkQuery) query).runUnlinkQuery(this.graphDatabase)) {
                                            response.put("@response", "OK");
                                            this.graphDatabase.recordQuery(query);
                                        } else {
                                            response.put("@error", "FailedToUnlink");
                                        }
                                        break;
                                    }
                                    case DIRECTORY: {
                                        Map<String, Node> listMap = ((DirectoryQuery) query).runDirectoryQuery(this.graphDatabase);
                                        JSONObject nodeList = new JSONObject();
                                        for (String key : listMap.keySet()) {
                                            nodeList.put(key, listMap.get(key).getId());
                                        }
                                        response.put("@map", nodeList);
                                        break;
                                    }
                                    case PERMISSIONS: {
                                        AuthorizedAction[] listMap = ((PermissionsQuery) query).runPermissionsQuery(this.graphDatabase);
                                        response.put("@permissions", listMap);
                                        break;
                                    }
                                    case DELETE: {
                                        //System.out.println("DELETE QUERY = " + query.toString());
                                        if (((DeleteQuery) query).runDeleteQuery(this.graphDatabase)) {
                                            response.put("@response", "OK");
                                        } else {
                                            response.put("@error", "FailedToDeleteAll");
                                        }
                                        this.graphDatabase.recordQuery(query);
                                        break;
                                    }
                                    case REFERENCES: {
                                        Map<String, Node[]> listMap = ((ReferencesQuery) query).runReferencesQuery(this.graphDatabase);
                                        JSONObject nodeList = new JSONObject();
                                        for (String key : listMap.keySet()) {
                                            JSONArray nodeArr = new JSONArray();
                                            for (Node oNode : listMap.get(key)) {
                                                nodeArr.put(oNode.getId());
                                            }
                                            nodeList.put(key, nodeArr);
                                        }
                                        response.put("@map", nodeList);
                                        break;
                                    }
                                }
                            } catch (QueryException queryException) {
                                response.put("@error", queryException.toString());
                            }
                            break;
                        case "@new":
                            JSONObject bodyJSONObject = new JSONObject(bodyContent);
                            Node newNode = this.graphDatabase.newNode(
                                bodyJSONObject.has("@data") ? (bodyJSONObject.isNull("@data") ? null : bodyJSONObject.getString("@data")) : null,
                                userNode,
                                bodyJSONObject.has("@schema") ? (bodyJSONObject.isNull("@schema") ? null : bodyJSONObject.getString("@schema")) : null
                            );
                            System.out.println(newNode.getCNode().getId());
                            response.put("@id", newNode.getId());
                            break;
                        case "@server_info":
                            response.put("@instance_id", this.graphDatabase.getInstanceId().toString());
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
                    tailNode = new RelativeNodePath(String.join("/", reconstruct)).toAbsolute(new NodePathContext(userNode, userNode)).getNodeFrom(this.graphDatabase, securityContext);
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
                    response = nodeToJson(securityContext, tailNode);
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
