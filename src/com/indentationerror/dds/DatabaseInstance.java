package com.indentationerror.dds;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.Ed25519Signer;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetKeyPair;
import com.nimbusds.jose.jwk.gen.JWKGenerator;
import com.nimbusds.jose.jwk.gen.OctetKeyPairGenerator;
import com.nimbusds.jwt.JWTParser;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;

public class DatabaseInstance {
    protected HashMap<UUID, Node> registeredNodes;
    private UUID instanceId;
    private String dbLocation;

    private OctetKeyPair jwk;

    private Queue<JournalSegment> completeJournal;
    private JournalSegment currentJournalSegment;

    private long maxJournalAgeMs = 1000 * 60; // Rotate journals every 60 seconds by default

    private static byte[] NAMESPACE_DNS = UUIDUtils.asBytes(UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));
    public DatabaseInstance(String configFilePath) throws IOException, NoSuchAlgorithmException, UnvalidatedJournalSegment {
        File configFile = new File(configFilePath);
        StringBuilder jsonBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                jsonBuilder.append(currentLine).append("\n");
            }
        }
        JSONObject configObject = new JSONObject(jsonBuilder.toString());
        String fqdn = configObject.getString("fqdn");
        if (configObject.has("journal_lifetime")) {
            maxJournalAgeMs = 1000 * configObject.getInt("journal_lifetime");
        }
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

        this.instanceId = UUIDUtils.asUUID(result);

        this.registeredNodes = new HashMap<>();

        if (!configObject.getString("data_directory").endsWith(File.separator)) {
            this.dbLocation = configObject.getString("data_directory") + File.separator;
        } else {
            this.dbLocation = configObject.getString("data_directory");
        }
        File dbDirectory = new File(this.dbLocation);
        if (!dbDirectory.exists()) {
            dbDirectory.mkdirs();
        }
        if (!dbDirectory.isDirectory()) {
            throw new FileAlreadyExistsException("The intended database directory path already points to a file");
        }
        this.completeJournal = new LinkedList<>();
        File[] directoryListing = dbDirectory.listFiles();
        Arrays.sort(directoryListing);
        for (File child : directoryListing) {
            if (child.getName().endsWith(".njs") && child.canRead()) {
                System.out.println("Replaying journal segment [" + child.getName() + "]");
                JournalSegment journalSegment = new JournalSegment(this, child);
                journalSegment.replayOn(this);
                this.completeJournal.offer(journalSegment);
            }
        }
        System.out.println("Journal rebuilt!");

        //https://www.javadoc.io/doc/com.nimbusds/nimbus-jose-jwt/latest/index.html

        File jwkFile = new File(this.dbLocation + this.instanceId.toString() + ".private.jwk");

        if (jwkFile.exists() && jwkFile.isFile() && jwkFile.canRead()) {
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
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        if (this.jwk == null) {
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
            } catch (JOSEException e) {
                throw new RuntimeException(e);
            }
        }

        this.currentJournalSegment = new JournalSegment(this);

        for (UUID nodeId : registeredNodes.keySet()) {
            System.out.println(nodeId);
        }
    }

    public String signPayload(byte[] payload) {
        try {
            JWSSigner signer = new Ed25519Signer(jwk);
            JWSObject jwsObject = new JWSObject(new JWSHeader.Builder(JWSAlgorithm.EdDSA).keyID(this.jwk.getKeyID()).build(), new Payload(payload));
            jwsObject.sign(signer);
            return jwsObject.serialize();
        } catch (JOSEException e) {
            throw new RuntimeException(e);
        }
        //JWSVerifier verifier = new Ed25519Verifier(publicJWK);
    }
    public Node newNode(String dataUri, Node creator, String schema) {
        try {
            UUID localId = null;
            WUUID globalId;
            boolean success = false;
            while (!success) {
                localId = UUID.randomUUID();
                success = this.getNode(localId) == null; // Check this id doesn't exist already
            }
            globalId = new WUUID(this.getInstanceId(), localId);
            WUUID globalCreator;
            if (creator != null) {
                globalCreator = creator.getGlobalId();
            } else {
                globalCreator = new WUUID(this.getInstanceId(), new UUID(0, 0));
            }
            Node node = new Node(localId, globalId, creator, globalCreator, dataUri, schema);
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

    void registerNode(Node node) {
        this.registeredNodes.put(node.getId(), node);
    }

    public Node getNode(WUUID id) {
        if (this.registeredNodes.containsKey(id.getOriginalNodeId())) {
            return this.registeredNodes.get(id.getOriginalNodeId());
        }
        Iterator<Node> nodes = this.registeredNodes.values().iterator();
        while (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.getGlobalId().equals(id)) {
                return node;
            }
        }
        return null;
    }

    public Node getNode(UUID id) {
        if (this.registeredNodes.containsKey(id)) {
            return this.registeredNodes.get(id);
        }
        return null;
    }

    public void shutdown() throws IOException {
        this.getOpenJournal().close();
    }
}
