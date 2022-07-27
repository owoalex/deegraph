package com.indentationerror.dds.database;

import com.indentationerror.dds.conditions.EqualityCondition;
import com.indentationerror.dds.conditions.RawValue;
import com.indentationerror.dds.exceptions.ClosedJournalException;
import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import com.indentationerror.dds.exceptions.UnvalidatedJournalSegment;
import com.indentationerror.dds.formats.UUIDUtils;
import com.indentationerror.dds.query.Query;
import com.indentationerror.dds.server.AuthenticationMethod;
import com.indentationerror.dds.server.SharedSecretAuthentication;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.Ed25519Signer;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetKeyPair;
import com.nimbusds.jose.jwk.gen.JWKGenerator;
import com.nimbusds.jose.jwk.gen.OctetKeyPairGenerator;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphDatabase {
    private ArrayList<AuthorizationRule> authorizationRules; // Stores all the parsed authorization rules used for generating permissions
    private HashMap<String, ArrayList<AuthenticationMethod>> authenticationMethods;

    private HashMap<String, ArrayList<AuthorizationRule>> relevantRule; // To save searching for rules that matter between two nodes every request, cache the result of the search

    private HashMap<UUID, Node> registeredNodes; // Stores all the nodes we have loaded in the database

    //private ArrayList<ArrayList<UUID>> missingNodes; // Stores node global ids that are referenced but not found in database.

    private UUID instanceId;
    private String instanceFqdn;
    private String dbLocation;

    private OctetKeyPair jwk;

    private Queue<JournalSegment> completeJournal;
    private JournalSegment currentJournalSegment;

    private long maxJournalAgeMs = 1000 * 60; // Rotate journals every 60 seconds by default

    private JSONObject config;

    private static byte[] NAMESPACE_DNS = UUIDUtils.asBytes(UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));

    public GraphDatabase(String configFilePath) throws UnvalidatedJournalSegment, IOException, NoSuchAlgorithmException {
        File configFile = new File(configFilePath);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        this.config = new JSONObject(jsonBuilder.toString());

        this.authenticationMethods = new HashMap<>();
        this.authorizationRules = new ArrayList<>();
        this.relevantRule = new HashMap<>();
        this.relevantRule.put("*", new ArrayList<>());

        //String instanceNodePath = "{" + graphDatabaseBacking.getInstanceId().toString() + "}";

        //
        // Default rule whereby the creator of a node has full perms on the node
        //this.relevantRule.get("*").add(new AuthorizationRule(new EqualityCondition(this, new RawValue(this, "@creator_id"), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.GRANT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));
        // Default rule whereby a node has full perms on itself
        //this.relevantRule.get("*").add(new AuthorizationRule(new EqualityCondition(this, new RawValue(this, "@id"), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.GRANT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));

        this.instanceFqdn = this.config.getString("fqdn");
        if (this.config.has("journal_lifetime")) {
            maxJournalAgeMs = 1000 * this.config.getInt("journal_lifetime");
        }
        byte[] fqdnBytes = this.instanceFqdn.getBytes();
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] compound = new byte[NAMESPACE_DNS.length + fqdnBytes.length];
        System.arraycopy(NAMESPACE_DNS, 0, compound, 0, NAMESPACE_DNS.length);
        System.arraycopy(fqdnBytes, 0, compound, NAMESPACE_DNS.length, fqdnBytes.length);
        byte[] hash = md.digest(compound);
        byte[] result = new byte[16];
        // copy first 16-bytes of the hash into our Uuid result
        System.arraycopy(hash, 0, result, 0, result.length);
        // set high-nibble to 5 to indicate type 5
        result[6] &= 0x0F;
        result[6] |= 0x50;

        // set upper two bits to "10"
        result[8] &= 0x3F;
        result[8] |= 0x80;

        this.instanceId = UUIDUtils.asUUID(result);

        this.registeredNodes = new HashMap<>();

        if (!this.config.getString("data_directory").endsWith(File.separator)) {
            this.dbLocation = this.config.getString("data_directory") + File.separator;
        } else {
            this.dbLocation = this.config.getString("data_directory");
        }
        File dbDirectory = new File(this.dbLocation);
        if (!dbDirectory.exists()) {
            dbDirectory.mkdirs();
        }
        if (!dbDirectory.isDirectory()) {
            throw new FileAlreadyExistsException("The intended database directory path already points to a file");
        }

        //https://www.javadoc.io/doc/com.nimbusds/nimbus-jose-jwt/latest/index.html

        File jwkFile = new File(this.dbLocation + this.instanceId.toString() + ".private.jwk");

        if (jwkFile.exists() && jwkFile.isFile() && jwkFile.canRead()) {
            System.out.println("Loading private key");
            jsonBuilder = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(jwkFile))) {
                String currentLine;
                while ((currentLine = br.readLine()) != null) {
                    jsonBuilder.append(currentLine).append("\n");
                }
            }
            JSONObject jsonObject = new JSONObject(jsonBuilder.toString());

            //System.out.println(jsonBuilder.toString());

            try {
                this.jwk = (OctetKeyPair) JWK.parse(jsonObject.toMap());
                System.out.println("Private key loaded successfully");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        if (this.jwk == null) {
            System.out.println("Generating a private key for the server");
            JWKGenerator<OctetKeyPair> jwkGenerator = new OctetKeyPairGenerator(Curve.Ed25519);
            jwkGenerator.algorithm(new Algorithm("EdDSA"));
            jwkGenerator.keyUse(KeyUse.SIGNATURE);
            jwkGenerator.keyID(UUID.randomUUID().toString());
            try {
                this.jwk = jwkGenerator.generate();
                Map<String, Object> jwkObj = jwk.toJSONObject();
                JSONObject jsonObject = new JSONObject(jwkObj);
                FileWriter jwkWriter = new FileWriter(jwkFile);
                jwkWriter.write(jsonObject.toString(4));
                jwkWriter.close();
                File publicJWKFile = new File(this.dbLocation + this.instanceId.toString() + ".public.jwk");
                jwkObj = jwk.toPublicJWK().toJSONObject();
                jsonObject = new JSONObject(jwkObj);
                jwkWriter = new FileWriter(publicJWKFile);
                jwkWriter.write(jsonObject.toString(4));
                jwkWriter.close();
                Files.copy(publicJWKFile.toPath(), Paths.get(this.instanceId.toString() + ".public.jwk"), StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Private key OK");
            } catch (JOSEException e) {
                throw new RuntimeException(e);
            }
        }

        this.currentJournalSegment = new JournalSegment(this);

        this.getRelevantRule().put("{" + this.instanceId.toString() + "}", new ArrayList<>());
        // Default rule whereby the root node can access everything
        this.getRelevantRule().get("{" + this.instanceId.toString() + "}").add(new AuthorizationRule(new EqualityCondition(this, new RawValue(this, "\"{" + this.instanceId.toString() + "}\""), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.ACT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));

        System.out.println("Restoring journal from disk");
        this.completeJournal = new LinkedList<>();
        File[] directoryListing = dbDirectory.listFiles();
        Arrays.sort(directoryListing);
        boolean journalEmpty = true;
        for (File child : directoryListing) {
            if (child.getName().endsWith(".njs") && child.canRead()) {
                if (child.length() > 0) { // Don't replay an empty journal file! - This can happen on an unclean shutdown
                    System.out.println("Replaying journal segment [" + child.getName() + "]");
                    JournalSegment journalSegment = new JournalSegment(this, child);
                    journalSegment.replayOn(this, false);
                    this.completeJournal.offer(journalSegment);
                    journalEmpty = false;
                }
            }
            if (child.getName().endsWith(".journal.dgc") && child.canRead()) {
                if (child.length() > 0) { // Don't try to read an empty container file! - This can happen on an unclean shutdown
                    System.out.println("Replaying v2 journal segment [" + child.getName() + "]");
                    JournalSegment journalSegment = JournalSegment.fromDumpV2(this, new BufferedReader(new FileReader(child)));
                    journalSegment.replayOn(this, false);
                    this.completeJournal.offer(journalSegment);
                    journalEmpty = false;
                }
            }
        }
        if (journalEmpty) {
            System.out.println("Journal empty");
        } else {
            System.out.println("Journal rebuilt");
        }

        if (this.getNodeUnsafe(this.instanceId) == null) {
            System.out.println("No instance node found");
            try {
                Node instanceNode = new Node(this, this.instanceId, this.instanceId, this.instanceId, null, null, null, null);
                instanceNode.makeSelfReferential();
                getOpenJournal().registerNewNode(instanceNode);
                System.out.println("Created instance node");
            } catch (ClosedJournalException e) {
                throw new RuntimeException(e);
            } catch (DuplicateNodeStoreException e) {
                throw new RuntimeException(e);
            }
        }

        for (UUID nodeId : registeredNodes.keySet()) {
            System.out.println(nodeId);
        }

        if (this.getConfig().has("root_auth_tokens")) {
            if (!this.authenticationMethods.containsKey(this.getInstanceId())) {
                this.authenticationMethods.put("*", new ArrayList<>());
            }
            for (Object token : this.getConfig().getJSONArray("root_auth_tokens").toList()) {
                if (token instanceof String) {
                    this.authenticationMethods.get("*").add(new SharedSecretAuthentication((String) token));
                }
            }
        }
    }

    HashMap<String, ArrayList<AuthorizationRule>> getRelevantRule() {
        return this.relevantRule;
    }

    public List<AuthenticationMethod> getAuthMethods(Node node) {
        if (node == null) {
            return this.authenticationMethods.get("*");
        }
        List<AuthenticationMethod> exact = this.authenticationMethods.get("{" + node.getId() + "}");
        List<AuthenticationMethod> global = this.authenticationMethods.get("*");
        return (exact == null) ? global : Stream.concat(exact.stream(), global.stream()).collect(Collectors.toList());
    }

    public void registerRule(AuthorizationRule rule, String match) {
        authorizationRules.add(rule);
        relevantRule.get(match).add(rule);
    }

    public AuthorizedAction[] getPermsOnNode(Node actor, Node object) {
        ArrayList<AuthorizedAction> authorizedActions = new ArrayList<>();
        ArrayList<Authorization> authorizations = new ArrayList<>();
        ArrayList<AuthorizationRule> rules = new ArrayList<>();

        rules.addAll(this.relevantRule.get("*"));
        if (this.relevantRule.containsKey("{" + actor.getId().toString() + "}")) {
            rules.addAll(this.relevantRule.get("{" + actor.getId().toString() + "}"));
        }
        if (this.relevantRule.containsKey("{" + object.getId().toString() + "}")) {
            rules.addAll(this.relevantRule.get("{" + object.getId().toString() + "}"));
        }

        for (AuthorizationRule rule : rules) {
            authorizations.addAll(Arrays.asList(rule.getAuthorizations(actor, object)));
        }

        for (Authorization authorization : authorizations) {
            //System.out.println(authorization.getAction().toString());
            authorizedActions.addAll(Arrays.asList(authorization.getAction()));
        }

        List<AuthorizedAction> uniqueAuthorizedActions = authorizedActions.stream().distinct().collect(Collectors.toList());

        return uniqueAuthorizedActions.toArray(new AuthorizedAction[0]);
    }

    public JSONObject getConfig() {
        return this.config;
    }

    public String signPayload(byte[] payload) {
        return this.signPayloadRaw(payload).serialize();
    }

    public JWSObject signPayloadRaw(byte[] payload) {
        try {
            JWSSigner signer = new Ed25519Signer(jwk);
            JWSObject jwsObject = new JWSObject(new JWSHeader.Builder(JWSAlgorithm.EdDSA).keyID(this.jwk.getKeyID()).build(), new Payload(payload));
            jwsObject.sign(signer);
            return jwsObject;
        } catch (JOSEException e) {
            throw new RuntimeException(e);
        }
        //JWSVerifier verifier = new Ed25519Verifier(publicJWK);
    }

    public Node[] getAllNodesUnsafe() {
        return registeredNodes.values().toArray(new Node[0]);
    }

    public Node newNode(String dataUri, Node creator, String schema) {
        try {
            UUID localId = null;
            boolean success = false;
            while (!success) {
                localId = UUID.randomUUID();
                success = this.getNodeUnsafe(localId) == null; // Check this id doesn't exist already
            }
            if (creator == null) {
                creator = this.getNodeUnsafe(this.getInstanceId()); // Try and capture the creator node
            }
            UUID originalCreator = creator.getId();
            Node node = new Node(this, localId, localId, this.getInstanceId(), creator, originalCreator, dataUri, schema);
            getOpenJournal().registerNewNode(node);
            return node;
        } catch (DuplicateNodeStoreException e) {
            throw new RuntimeException(e); // This *should* never happen anyway
        } catch (ClosedJournalException e) {
            throw new RuntimeException(e);
        }
    }

    String getDbLocation() {
        return this.dbLocation;
    }

    public void recordQuery(Query query) throws ClosedJournalException {
        getOpenJournal().registerQuery(query);
        //System.out.println("RQ: " + query.toString());
    }

    private JournalSegment getOpenJournal() {
        if (currentJournalSegment.getOpenDate().before(new Date(System.currentTimeMillis() - maxJournalAgeMs))) {
            try {
                currentJournalSegment.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (!currentJournalSegment.isOpen()) {
            completeJournal.offer(currentJournalSegment);
            try {
                currentJournalSegment = new JournalSegment(this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return currentJournalSegment;
    }

    public UUID getInstanceId() {
        return instanceId;
    }

    public String getInstanceFqdn() {
        return instanceFqdn;
    }

    void registerNodeUnsafe(Node node) {
        this.registeredNodes.put(node.getId(), node);
    }

    public void unregisterNodeUnsafe(Node node) {
        this.registeredNodes.remove(node.getId());
    }

    public Node getNodeUnsafe(UUID originalId, UUID originalInstanceId) {
        if (originalInstanceId.equals(this.getInstanceId())) {
            return getNodeUnsafe(originalId);
        }
        Iterator<Node> nodes = this.registeredNodes.values().iterator();
        while (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.getOriginalInstanceId().equals(originalInstanceId)) {
                if (node.getOriginalId().equals(originalId)) {
                    return node;
                }
            }
        }
        return null;
    }

    public Node getNodeUnsafe(UUID id) {
        if (this.registeredNodes.containsKey(id)) {
            return this.registeredNodes.get(id);
        }
        return null;
    }

    public void shutdown() throws IOException {
        this.getOpenJournal().close();
    }

    public Node getInstanceNode() {
        return this.getNodeUnsafe(this.instanceId);
    }
}
