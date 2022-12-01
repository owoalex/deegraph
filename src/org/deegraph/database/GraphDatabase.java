package org.deegraph.database;

import org.deegraph.conditions.EqualityCondition;
import org.deegraph.conditions.RawValue;
import org.deegraph.exceptions.ClosedJournalException;
import org.deegraph.exceptions.DuplicateNodeStoreException;
import org.deegraph.exceptions.UnvalidatedJournalSegment;
import org.deegraph.formats.UUIDUtils;
import org.deegraph.query.Query;
import org.deegraph.server.AuthenticationMethod;
import org.deegraph.server.SharedSecretAuthentication;
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

    private HashMap<UUID, ArrayList<OctetKeyPair>> instanceTrustStore; // Stores instance public keys we trust
    private HashMap<String, ArrayList<AuthenticationMethod>> authenticationMethods;

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

    private boolean debugMode = false;

    public GraphDatabase(String configFilePath) throws UnvalidatedJournalSegment, IOException {
        this(configFilePath, false);
    }

    public GraphDatabase(String configFilePath, boolean debugMode) throws UnvalidatedJournalSegment, IOException {
        this.debugMode = debugMode;
        File configFile = new File(configFilePath);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        this.config = new JSONObject(jsonBuilder.toString());

        this.instanceTrustStore = new HashMap<>();
        this.authenticationMethods = new HashMap<>();
        this.authorizationRules = new ArrayList<>();

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
        this.instanceId = GraphDatabase.instanceFQDNToUUID(this.instanceFqdn);

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

        ArrayList<OctetKeyPair> ownKeyList = new ArrayList<>();
        ownKeyList.add(this.jwk.toPublicJWK());
        this.instanceTrustStore.put(this.instanceId, ownKeyList);

        this.currentJournalSegment = new JournalSegment(this);

        // Default rule whereby the root node can access everything - was needed in the past, now handled in a more efficient manner
        //registerRule(new AuthorizationRule(new RelativeNodePath[] {new RelativeNodePath("**")}, new EqualityCondition(this, new RawValue(this, "\"{" + this.instanceId.toString() + "}\""), new RawValue(this, "/@id")), new AuthorizedAction[] {AuthorizedAction.ACT, AuthorizedAction.DELEGATE, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE}));

        System.out.println("Restoring journal from disk");
        this.completeJournal = new LinkedList<>();
        File[] directoryListing = dbDirectory.listFiles();
        Arrays.sort(directoryListing);
        boolean journalEmpty = true;
        for (File child : directoryListing) {
            if (child.getName().endsWith(".journal.dgc") && child.canRead()) {
                if (child.length() > 0) { // Don't try to read an empty container file! - This can happen on an unclean shutdown
                    try {
                        System.out.println("Replaying journal segment [" + child.getName() + "]");
                        JournalSegment journalSegment = new JournalSegment(this, new BufferedReader(new FileReader(child)));
                        journalSegment.replay(false);
                        this.completeJournal.offer(journalSegment);
                        journalEmpty = false;
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
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
                Node instanceNode = new Node(this, this.instanceId, this.instanceId, this.instanceId, null, this.instanceId, null, null);
                instanceNode.makeSelfReferential();
                getOpenJournal().registerNewNode(instanceNode);
                System.out.println("Created instance node");
            } catch (ClosedJournalException e) {
                throw new RuntimeException(e);
            } catch (DuplicateNodeStoreException e) {
                throw new RuntimeException(e);
            }
        }

        //for (UUID nodeId : registeredNodes.keySet()) {
        //    System.out.println(nodeId);
        //}

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


    public OctetKeyPair[] getInstanceKeys(UUID instanceId) {
        return instanceTrustStore.containsKey(instanceId) ? instanceTrustStore.get(instanceId).toArray(new OctetKeyPair[0]) : new OctetKeyPair[0];
    }

    public OctetKeyPair[] getInstanceKeys(String instanceId) {
        return getInstanceKeys(GraphDatabase.instanceFQDNToUUID(instanceId));
    }

    public static UUID instanceFQDNToUUID(String fqdn) {
        try {
            byte[] fqdnBytes = fqdn.getBytes();
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

            return UUIDUtils.asUUID(result);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean registerPeerNodeKey(String fqdn, OctetKeyPair ocp, Node actor) {
        if (this.installPeerNodeKey(fqdn, ocp, actor)) {
            //this.getOpenJournal().
            while (true) {
                try {
                    this.getOpenJournal().registerEntry(new TrustKeyJournalEntry(ocp, fqdn, actor.getId()));
                    break;
                } catch (ClosedJournalException e) {}
            }
        }
        return false;
    }

    public boolean installPeerNodeKey(String fqdn, OctetKeyPair ocp, Node actor) {
        if (Arrays.asList(this.getPermsOnNode(actor, this.getInstanceNode())).contains(AuthorizedAction.ACT)) { // Make sure the actor trying to put this rule into place has root perms
            UUID peerId = GraphDatabase.instanceFQDNToUUID(fqdn);
            if (!this.instanceTrustStore.containsKey(peerId)) {
                this.instanceTrustStore.put(peerId, new ArrayList<>());
            }
            if (!this.registeredNodes.containsKey(peerId)) {
                Node node = new Node(this, peerId, peerId, peerId, actor, peerId, null, null);
                this.registerNodeUnsafe(node);
            }
            //this.getNodeUnsafe(peerId)
            this.instanceTrustStore.get(peerId).add(ocp);
        }
        return false;
    }

    public List<AuthenticationMethod> getAuthMethods(Node node) {
        if (node == null) {
            return this.authenticationMethods.get("*");
        }
        List<AuthenticationMethod> exact = this.authenticationMethods.get("{" + node.getId() + "}");
        List<AuthenticationMethod> global = this.authenticationMethods.get("*");
        return (exact == null) ? global : Stream.concat(exact.stream(), global.stream()).collect(Collectors.toList());
    }

    public void registerRule(AuthorizationRule rule) {
        authorizationRules.add(rule);
    }

    public Authorization[] getAuthorizationsForNode(Node actor, Node object) {
        if (actor == null) { // Obviously if either is null then no permissions can be given
            //System.out.println("Perms for NULL on ANY = []");
            return new Authorization[0];
        }
        if (object == null) {
            //System.out.println("Perms for ANY on NULL = []");
            return new Authorization[0];
        }

        ArrayList<Authorization> authorizations = new ArrayList<>();

        for (AuthorizationRule rule : this.authorizationRules) {
            Authorization auth = rule.getAuthorization(this, actor, object);
            if (auth != null) {
                authorizations.add(auth);
            }
            //System.out.println(rule.getCondition());
        }

        return authorizations.toArray(new Authorization[0]);
    }
    public AuthorizedAction[] getPermsOnNode(Node actor, Node object) {
        if (this.getInstanceNode().equals(actor)) {
            AuthorizedAction[] rootAuth = new AuthorizedAction[] {AuthorizedAction.ACT, AuthorizedAction.DELETE, AuthorizedAction.READ, AuthorizedAction.WRITE};
            //System.out.println("Perms for ROOT on ANY = *");
            return rootAuth; // Root user always has all perms - so we can just exit here without evaluating any rules
        }

        if (this.debugMode) {
            System.out.println("ACTOR " + actor.getId());
        }

        Authorization[] authorizations = getAuthorizationsForNode(actor, object);

        ArrayList<AuthorizedAction> authorizedActions = new ArrayList<>();

        for (Authorization authorization: authorizations) {
            if (authorization.isValidForNode(object)) {
                authorizedActions.addAll(Arrays.asList(authorization.getActions()));
            } else {
                if (this.debugMode) {
                    System.out.println("NOT VALID FOR " + object.getId());
                }
            }
        }

        List<AuthorizedAction> uniqueAuthorizedActions = authorizedActions.stream().distinct().collect(Collectors.toList());

        /*String permsStr = "";
        for (AuthorizedAction authAct: uniqueAuthorizedActions.toArray(new AuthorizedAction[0])) {
            permsStr = permsStr + ((permsStr.length() == 0) ? "" : ", ") + authAct;
        }
        System.out.println("Perms for {" + actor.getId() + "} on {" + object.getId() + "} = [" + permsStr + "]"); */

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
        getOpenJournal().registerEntry(new QueryJournalEntry(query.toString(), query.getActor()));
        //System.out.println("RQ: " + query.toString());
    }

    public JournalSegment getOpenJournal() {
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

    public Node[] getAllVisibleNodes(Node actor) {
        if (actor == null) {
            return new Node[0];
        }
        if (getInstanceNode().equals(actor)) {
            return this.getAllNodesUnsafe();
        }
        ArrayList<Node> allNodes = new ArrayList<>(this.registeredNodes.values());
        ArrayList<Node> searchSpace = new ArrayList<>(this.registeredNodes.values());
        ArrayList<Node> validNodes = new ArrayList<>();

        for (AuthorizationRule rule : this.authorizationRules) {
            if (Arrays.asList(rule.getAuthorizableActions()).contains(AuthorizedAction.READ)) {
                for (Node candidate: allNodes) {
                    Authorization auth = rule.getAuthorization(this, actor, candidate);
                    for (RelativeNodePath relativeNodePath: auth.getValidPaths()) {
                        List<Node> newValidNodes = Arrays.asList(relativeNodePath.getMatchingNodes(new SecurityContext(this, this.getInstanceNode()), new NodePathContext(actor, candidate), searchSpace.toArray(new Node[0])));
                        searchSpace.removeAll(newValidNodes);
                        validNodes.addAll(newValidNodes);
                    }
                }
            }
        }
        return validNodes.toArray(new Node[0]);
    }

    public boolean getDebugSetting() {
        return debugMode;
    }

    public Node getNode(UUID uuid, Node actor) {
        if (actor == null) {
            return null;
        }
        if (getInstanceNode().equals(actor)) {
            return this.getNodeUnsafe(uuid);
        }
        if (Arrays.asList(this.getPermsOnNode(actor, this.getNodeUnsafe(uuid))).contains(AuthorizedAction.READ)) {
            return this.getNodeUnsafe(uuid);
        } else {
            return null;
        }
    }
}
