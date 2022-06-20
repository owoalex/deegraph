package com.indentationerror.dds.database;

import com.indentationerror.dds.conditions.EqualityCondition;
import com.indentationerror.dds.conditions.RawValue;
import com.indentationerror.dds.exceptions.UnvalidatedJournalSegment;
import com.indentationerror.dds.formats.WUUID;
import com.indentationerror.dds.server.AuthenticationMethod;
import com.indentationerror.dds.server.SharedSecretAuthentication;
import org.json.JSONObject;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphDatabase {
    private GraphDatabaseBacking graphDatabaseBacking;

    private ArrayList<AuthorizationRule> authorizationRules; // Stores all the parsed authorization rules used for generating permissions
    private HashMap<String, ArrayList<AuthenticationMethod>> authenticationMethods;

    private HashMap<String, ArrayList<AuthorizationRule>> relevantRule; // To save searching for rules that matter between two nodes every request, cache the result of the search

    public GraphDatabase(GraphDatabaseBacking graphDatabaseBacking) throws UnvalidatedJournalSegment, IOException, NoSuchAlgorithmException {
        this.graphDatabaseBacking = graphDatabaseBacking;
        this.graphDatabaseBacking.init(this);
        this.authenticationMethods = new HashMap<>();
        this.authorizationRules = new ArrayList<>();
        this.relevantRule = new HashMap<>();
        this.relevantRule.put("*", new ArrayList<>());
        String instanceNodePath = "{" + graphDatabaseBacking.getInstanceId().toString() + "}";
        this.relevantRule.put(instanceNodePath, new ArrayList<>());

        // Default rule whereby the root node can access everything
        this.relevantRule.get(instanceNodePath).add(new AuthorizationRule(new EqualityCondition(this, new RawValue(this, instanceNodePath), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.GRANT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));

        this.relevantRule.get("*").add(new AuthorizationRule(new EqualityCondition(this, new RawValue(this, "@creator_id"), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.GRANT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));

        if (this.graphDatabaseBacking.getConfig().has("root_auth_tokens")) {
            if (!this.authenticationMethods.containsKey(this.graphDatabaseBacking.getInstanceId())) {
                this.authenticationMethods.put("*", new ArrayList<>());
            }
            for (Object token : this.graphDatabaseBacking.getConfig().getJSONArray("root_auth_tokens").toList()) {
                if (token instanceof String) {
                    this.authenticationMethods.get("*").add(new SharedSecretAuthentication((String) token));
                }
            }
        }
    }

    public List<AuthenticationMethod> getAuthMethods(Node node) {
        List<AuthenticationMethod> exact = this.authenticationMethods.get("{" + node.getId() + "}");
        List<AuthenticationMethod> global = this.authenticationMethods.get("*");
        return (exact == null) ? global : Stream.concat(exact.stream(), global.stream()).collect(Collectors.toList());
    }

    public GraphDatabase(String configFilePath) throws IOException, NoSuchAlgorithmException, UnvalidatedJournalSegment {
        this(new GraphDatabaseBacking(configFilePath));
    }

    private void registerRule(AuthorizationRule rule) {
        authorizationRules.add(rule);
        relevantRule.get("*").add(rule);
    }

    public AuthorizedAction[] getPermsOnNode(Node actor, Node object) {
        ArrayList<AuthorizedAction> authorizedActions = new ArrayList<>();
        ArrayList<Authorization> authorizations = new ArrayList<>();
        ArrayList<AuthorizationRule> rules = new ArrayList<>();

        rules.addAll(this.relevantRule.get("*"));
        if (this.relevantRule.containsKey(actor.getId().toString())) {
            rules.addAll(this.relevantRule.get(actor.getId().toString()));
        }
        if (this.relevantRule.containsKey(object.getId().toString())) {
            rules.addAll(this.relevantRule.get(object.getId().toString()));
        }

        for (AuthorizationRule rule : rules) {
            authorizations.addAll(Arrays.asList(rule.getAuthorizations(actor, object)));
        }

        for (Authorization authorization : authorizations) {
            authorizedActions.addAll(Arrays.asList(authorization.getAction()));
        }

        List<AuthorizedAction> uniqueAuthorizedActions = authorizedActions.stream().distinct().collect(Collectors.toList());

        return uniqueAuthorizedActions.toArray(new AuthorizedAction[0]);
    }

    public void shutdown() throws IOException {
        this.graphDatabaseBacking.shutdown();
    }

    public JSONObject getConfig() {
        return this.graphDatabaseBacking.getConfig();
    }

    public UUID getInstanceId() {
        return this.graphDatabaseBacking.getInstanceId();
    }

    public Node getNode(UUID id) {
        return this.graphDatabaseBacking.getNode(id);
    }

    public Node getNode(WUUID globalId) {
        return this.graphDatabaseBacking.getNode(globalId);
    }

    public Node newNode(String s, Node userNode, String s1) {
        return this.graphDatabaseBacking.newNode(s, userNode, s1);
    }

    public Node[] getAllNodes() {
        return this.graphDatabaseBacking.getAllNodes();
    }

    GraphDatabaseBacking getBacking() {
        return this.graphDatabaseBacking;
    }
}
