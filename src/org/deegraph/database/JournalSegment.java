package org.deegraph.database;

import org.deegraph.exceptions.ClosedJournalException;
import org.deegraph.exceptions.DuplicateNodeStoreException;
import org.deegraph.exceptions.MissingNodeException;
import org.deegraph.exceptions.UnvalidatedJournalSegment;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.Ed25519Verifier;
import com.nimbusds.jose.jwk.OctetKeyPair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class JournalSegment {
    private Queue<JournalEntry> segmentActions;
    private boolean open;
    private Date openDate;
    private Date closeDate;
    private UUID journalUuid;
    private GraphDatabase graphDatabase;
    private UUID originalDatabaseInstanceId; // This should *only* be set if we're absolutely sure this segment has come from this id (we must have either created it or checked its signature)

    private File saveFile;
    private FileWriter saveFileWriter;

    public JournalSegment(GraphDatabase graphDatabase) throws IOException {
        this.segmentActions = new LinkedList<>();
        this.open = true;
        this.graphDatabase = graphDatabase;
        this.openDate = new Date();
        this.journalUuid = UUID.randomUUID();
        this.originalDatabaseInstanceId = graphDatabase.getInstanceId();
        this.saveFile = new File(graphDatabase.getDbLocation() + this.getId() + ".journal.dgc"); // deegraph container
        this.saveFile.createNewFile();
        this.saveFileWriter = new FileWriter(this.saveFile);
    }

    public JournalSegment(GraphDatabase gdb, BufferedReader input) throws IOException, UnvalidatedJournalSegment, ParseException {
        this.graphDatabase = gdb;
        this.segmentActions = new LinkedList<>();
        this.open = false;

        HashMap<String, ArrayList<String>> props = new HashMap<>();
        StringBuilder bodyStringBuilder = new StringBuilder();
        boolean bodyReached = false;
        while (true) {
            String line = input.readLine();
            if (line == null) {
                break;
            }
            if (bodyReached) {
                bodyStringBuilder.append(line);
                bodyStringBuilder.append("\r\n");
            } else {
                int delimiterPosition = line.indexOf(":");
                if (delimiterPosition != -1) {
                    String key = line.substring(0, delimiterPosition).toLowerCase(Locale.ROOT);
                    String value = line.substring(delimiterPosition + 1);
                    if (value.startsWith(" ")) {
                        value = value.substring(1);
                    }
                    ArrayList<String> values = new ArrayList<>();
                    String cValue = "";
                    boolean quoteMode = false;
                    while (value.length() > 0) {
                        char nChar = value.charAt(0);
                        value = value.substring(1);
                        switch (nChar) {
                            case '"':
                                quoteMode = !quoteMode;
                                break;
                            case ';':
                                if (!quoteMode) {
                                    if (cValue.startsWith(" ")) {
                                        cValue = cValue.substring(1);
                                    }
                                    values.add(cValue);
                                    cValue = "";
                                } else {
                                    cValue = cValue + nChar;
                                }
                                break;
                            case '=':
                                if (!quoteMode) {
                                    if (cValue.startsWith(" ")) {
                                        cValue = cValue.substring(1);
                                    }
                                    values.add(cValue + "=");
                                    cValue = "";
                                } else {
                                    cValue = cValue + nChar;
                                }
                                break;
                            case ' ':
                                if (!quoteMode) {
                                    break;
                                } else {
                                    cValue = cValue + nChar;
                                }
                                break;
                            case '\\':
                                nChar = value.charAt(0);
                                value = value.substring(1);
                            default:
                                cValue = cValue + nChar;
                        }
                    }
                    if (cValue.startsWith(" ")) {
                        cValue = cValue.substring(1);
                    }
                    values.add(cValue);
                    props.put(key, values);
                } else {
                    if (line.length() == 0) {
                        bodyReached = true;
                    } else {
                        System.err.println("Syntax error in dump header");
                    }
                }
            }
        }

        String body = bodyStringBuilder.toString().trim();

        System.out.println(new JSONObject(props).toString(4));
        System.out.println("===");
        System.out.println(body);

        UUID sourceInstanceId = UUID.fromString(props.get("origin-id").get(0));
        String sourceInstanceFqdn = props.get("origin-fqdn").get(0);
        String bodySignature = props.get("content-signature").get(0);
        int bodyLength = Integer.parseInt(props.get("content-length").get(0));

        if (bodyLength != body.length()) {
            throw new UnvalidatedJournalSegment("Could not validate journal segment - body length did not match expected value - is this journal segment corrupted?");
        }

        if (sourceInstanceId == null || sourceInstanceFqdn == null) {
            throw new UnvalidatedJournalSegment("Could not validate journal segment - source instance not identified - is this journal segment corrupted?");
        }

        if (!GraphDatabase.instanceFQDNToUUID(sourceInstanceFqdn).equals(sourceInstanceId)) {
            throw new UnvalidatedJournalSegment("Could not validate journal segment - source instance id does not map as expected to domain - is someone trying an attack here?");
        }

        boolean validatedSegment = false;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bodyHash = digest.digest(body.getBytes(StandardCharsets.UTF_8));
            String[] fullJWS = bodySignature.split("\\.");
            byte[] bodySignatureEmbeddedHash = Base64.getUrlDecoder().decode(fullJWS[1]);
            if (!Arrays.equals(bodySignatureEmbeddedHash, bodyHash)) {
                throw new UnvalidatedJournalSegment("Could not validate journal segment - hash did not match expected value - this journal segment is corrupted.");
            }

            JWSObject parsedJWSObject = null;
            try {
                bodySignature = fullJWS[0] + ".." + fullJWS[2]; // This is needed because nimbus-jose doesn't currently support verifying the hash itself - we've done this already though
                parsedJWSObject = JWSObject.parse(bodySignature, new Payload(bodyHash));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            //publicJWK = (OctetKeyPair) JWK.parse(jsonObject.toMap());
            for (OctetKeyPair ocp: this.graphDatabase.getInstanceKeys(sourceInstanceId)) {
                if (parsedJWSObject != null) {
                    try {
                        if (parsedJWSObject.verify(new Ed25519Verifier(ocp))) {
                            validatedSegment = true;
                            this.originalDatabaseInstanceId = sourceInstanceId; // We're now sure about where this has come from, so we can set this instance variable.
                        }
                    } catch (JOSEException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        if (validatedSegment) {
            JSONArray segmentDefinition = new JSONArray(body);

            for (Object entryDefinition : segmentDefinition) {
                if (entryDefinition instanceof JSONObject) {
                    this.segmentActions.add(JournalEntry.fromJson((JSONObject) entryDefinition));
                }
            }
        } else {
            throw new UnvalidatedJournalSegment("Could not validate journal segment - missing or invalid public key for origin {" + sourceInstanceFqdn.toString() + ":" + sourceInstanceId.toString() + "} - is this origin peered?");
        }
    }

    public void registerNewNode(Node node) throws ClosedJournalException, DuplicateNodeStoreException {
        if (this.open) {
            if (this.graphDatabase.getNodeUnsafe(node.getId()) == null && this.graphDatabase.getNodeUnsafe(node.getOriginalInstanceId(), node.getOriginalId()) == null) {
                this.segmentActions.add(new NewNodeJournalEntry(node));
                this.graphDatabase.registerNodeUnsafe(node);
            } else {
                throw new DuplicateNodeStoreException(node);
            }
        } else {
            throw new ClosedJournalException(this);
        }
    }

    public void registerEntry(JournalEntry journalEntry) throws ClosedJournalException {
        if (this.open) {
            this.segmentActions.add(journalEntry);
            //System.out.println(query.toString());
        } else {
            throw new ClosedJournalException(this);
        }
    }

    public boolean replay(boolean faultTolerant) {
        boolean valid = true;
        Node actingNode = this.graphDatabase.getNodeUnsafe(this.originalDatabaseInstanceId);
        for (JournalEntry journalEntry : this.segmentActions) {
            try {
                valid = valid && journalEntry.replayOn(this.graphDatabase, actingNode);
            } catch (MissingNodeException e) {
                if (!faultTolerant) {
                    throw new RuntimeException(e);
                }
            } catch (DuplicateNodeStoreException e) {
                if (!faultTolerant) {
                    throw new RuntimeException(e);
                }
            } catch (ParseException e) {
                if (!faultTolerant) {
                    throw new RuntimeException(e);
                }
            }
        }
        return valid;
    }

    @Override
    public String toString() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        StringBuilder sb = new StringBuilder();
        JSONArray journalOutput = new JSONArray();

        while (this.segmentActions.isEmpty() == false) {
            JournalEntry currentEntry = segmentActions.poll();
            JSONObject entryJson = currentEntry.asJson();
            journalOutput.put(entryJson);
        }

        String stringJournal = journalOutput.toString(4).replaceAll("\r", "").replaceAll("\n", "\r\n").trim();
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] journalAsBytes = stringJournal.getBytes(StandardCharsets.UTF_8);
        sb.append("deegraph-container-version: 1.0\r\n");
        sb.append("content-type: application/json\r\n");
        sb.append("origin-fqdn: " + this.graphDatabase.getInstanceFqdn() + "\r\n");
        sb.append("origin-id: " + this.graphDatabase.getInstanceId() + "\r\n");
        sb.append("content-signature: " + this.graphDatabase.signPayload(digest.digest(journalAsBytes)) + "\r\n");
        sb.append("content-length: " + stringJournal.length() + "\r\n");
        sb.append("\r\n");
        sb.append(stringJournal);
        return sb.toString();
    }
    public void close() throws IOException {
        if (this.open) { // If it's already closed, why close it again?
            this.closeDate = new Date();
            this.open = false;
            if (this.segmentActions.isEmpty()) {
                this.saveFileWriter.close();
                this.saveFile.delete();
            } else {
                String dump = this.toString();
                this.saveFileWriter.write(dump);
                this.saveFileWriter.close();
            }
            //new JournalSegment(this.saveFile);
        }
    }

    public Date getOpenDate() {
        return openDate;
    }

    public Date getCloseDate() {
        return closeDate;
    }

    public boolean isOpen() {
        return this.open;
    }

    public String getId() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss'Z'");
        df.setTimeZone(tz);
        return df.format(this.openDate) + "-" + originalDatabaseInstanceId.toString() + "-" + this.journalUuid.toString();
    }
}
