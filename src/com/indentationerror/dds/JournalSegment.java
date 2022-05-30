package com.indentationerror.dds;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.Ed25519Verifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetKeyPair;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class JournalSegment {
    private Queue<JournalEntry> segmentActions;
    private boolean localSegment;
    private boolean open;
    private Date openDate;
    private Date closeDate;
    private UUID journalUuid;
    private DatabaseInstance databaseInstance;
    private UUID originalDatabaseInstanceId;

    private File saveFile;
    private FileWriter saveFileWriter;

    public JournalSegment(DatabaseInstance databaseInstance) throws IOException {
        this.segmentActions = new LinkedList<>();
        this.open = true;
        this.databaseInstance = databaseInstance;
        this.openDate = new Date();
        this.journalUuid = UUID.randomUUID();
        this.originalDatabaseInstanceId = databaseInstance.getInstanceId();
        this.saveFile = new File(databaseInstance.getDbLocation() + this.getId() + ".njs"); // Node journal segment
        this.saveFile.createNewFile();
        this.saveFileWriter = new FileWriter(this.saveFile);
        this.localSegment = true;
    }

    public JournalSegment(DatabaseInstance databaseInstance, File file) throws IOException, UnvalidatedJournalSegment {
        this.databaseInstance = databaseInstance;
        this.segmentActions = new LinkedList<>();
        this.open = false;
        this.saveFile = file;
        this.localSegment = true;
        if (!fromDump(new BufferedReader(new FileReader(file)))) {
            throw new UnvalidatedJournalSegment();
        }
        // Parse old journal
    }

    public void registerNewNode(Node node) throws ClosedJournalException, DuplicateNodeStoreException {
        if (this.open) {
            if (this.databaseInstance.getNode(node.getId()) == null && this.databaseInstance.getNode(node.getGlobalId()) == null) {
                this.segmentActions.add(new NewNodeJournalEntry(node));
                this.databaseInstance.registerNode(node);
            } else {
                throw new DuplicateNodeStoreException(node);
            }
        } else {
            throw new ClosedJournalException(this);
        }
    }

    public void replayOn(DatabaseInstance databaseInstance) throws DuplicateNodeStoreException {
        for (JournalEntry journalEntry : this.segmentActions) {
            if (journalEntry instanceof NewNodeJournalEntry) {
                NewNodeJournalEntry newNodeJournalEntry = (NewNodeJournalEntry) journalEntry;
                newNodeJournalEntry.replayOn(databaseInstance);
            }
        }
    }

    private boolean fromDump(BufferedReader input) throws IOException, UnvalidatedJournalSegment {
        int mode = 0;
        String boundary = null;
        ArrayList<String> content = null;
        Stack<HashMap<String, ArrayList<String>>> propsStack = new Stack<>();
        HashMap<String, ArrayList<String>> props = new HashMap<>();
        StringBuilder bodyStringBuilder = new StringBuilder();
        String bodySignature = null;
        UUID sourceInstance = null;
        boolean bodyReached = false;
        while (true) {
            String line = input.readLine();
            if (line == null) {
                break;
            }
            if (bodyReached) {
                bodyStringBuilder.append(line);
                bodyStringBuilder.append("\r\n");
            }
            switch (mode) {
                case 1:
                    if (line.startsWith("--" + boundary)) {
                        //props.put("content", content);
                        props = propsStack.pop();
                        try {
                            if (props.containsKey("entry-type") && props.get("entry-type").size() != 0) {
                                switch (props.get("entry-type").get(0)) {
                                    case "new-node":
                                        Date creationDate = null;
                                        if (props.containsKey("creation-timestamp") && props.get("creation-timestamp").size() != 0) {
                                            String creationTimestamp = props.get("creation-timestamp").get(0);
                                            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(creationTimestamp);
                                            creationDate = Date.from(Instant.from(ta));
                                        }
                                        Date globalCreationDate = null;
                                        if (props.containsKey("global-creation-timestamp") && props.get("global-creation-timestamp").size() != 0) {
                                            String creationTimestamp = props.get("global-creation-timestamp").get(0);
                                            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(creationTimestamp);
                                            globalCreationDate = Date.from(Instant.from(ta));
                                        }
                                        WUUID globalId = null;
                                        if (props.get("global-content-id") != null) {
                                            globalId = WUUID.fromString(props.get("global-content-id").get(0));
                                        }
                                        WUUID oCNodeId = null;
                                        if (props.get("global-creator-id") != null) {
                                            oCNodeId = WUUID.fromString(props.get("global-creator-id").get(0));
                                        }
                                        UUID cNodeId = null;
                                        if (props.get("creator-id") != null) {
                                            cNodeId = UUID.fromString(props.get("creator-id").get(0));
                                        }
                                        UUID localId = null;
                                        if (props.get("content-id") != null) {
                                            localId = UUID.fromString(props.get("content-id").get(0));
                                        }
                                        String schema = null;
                                        if (props.get("schema") != null) {
                                            schema = props.get("schema").get(0);
                                        }
                                        String contentStr = null;
                                        if (content != null) {
                                            contentStr = String.join("\n", content.toArray(new String[0]));
                                        }

                                        NewNodeJournalEntry nodeJournalEntry = new NewNodeJournalEntry(localId, globalId, cNodeId, oCNodeId, contentStr, schema, creationDate, globalCreationDate);
                                        this.segmentActions.offer(nodeJournalEntry);
                                        break;
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Critical error processing journal segment " + this.journalUuid);
                            e.printStackTrace();
                        }
                        content = null;
                        mode = 0;
                    } else {
                        if (content == null) {
                            content = new ArrayList<>();
                        }
                        content.add(line);
                    }
                    break;
                case 0:
                default:
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
                        switch (key) {
                            case "content-type":
                                String type = values.remove(0);
                                if (type.startsWith("multipart/")) {
                                    int boundaryIdx = values.indexOf("boundary=") + 1;
                                    if (boundaryIdx > 0 && boundaryIdx < values.size()) {
                                        boundary = values.get(boundaryIdx);
                                    }
                                }
                                break;
                            case "body-signature":
                                bodySignature = values.remove(0);
                                break;
                            case "original-instance-id":
                                sourceInstance = UUID.fromString(values.remove(0));
                                break;
                            default:
                                props.put(key, values);
                                //System.out.println(key + " = " + String.join(" > ", values.toArray(new String[0])));
                        }
                    } else {
                        if (line.length() == 0) {
                            mode = 1;
                            bodyReached = true;
                            propsStack.push(props);
                            props = new HashMap<>();
                            //System.out.println("Content start");
                        } else {
                            System.err.println("Syntax error in dump header");
                        }
                    }
            }
        }

        if (sourceInstance == null) {
            throw new UnvalidatedJournalSegment("Could not validate journal segment - source instance not identified - is this journal segment corrupted?");
        }

        MessageDigest digest = null;
        try {
            OctetKeyPair publicJWK = null;
            JWSObject parsedJWSObject = null;
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bodyHash = digest.digest(bodyStringBuilder.toString().getBytes(StandardCharsets.UTF_8));

            File jwkFile = new File(this.databaseInstance.getDbLocation() + sourceInstance.toString() + ".public.jwk");

            if (jwkFile.exists() && jwkFile.isFile() && jwkFile.canRead()) {
                StringBuilder jsonBuilder = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new FileReader(jwkFile))) {
                    String currentLine;
                    while ((currentLine = br.readLine()) != null) {
                        jsonBuilder.append(currentLine).append("\n");
                    }
                }
                JSONObject jsonObject = new JSONObject(jsonBuilder.toString());

                try {
                    publicJWK = (OctetKeyPair) JWK.parse(jsonObject.toMap());
                    String[] fullJWS = bodySignature.split("\\.");
                    byte[] bodySignatureEmbeddedHash = Base64.getUrlDecoder().decode(fullJWS[1]);
                    if (!Arrays.equals(bodySignatureEmbeddedHash, bodyHash)) {
                        throw new UnvalidatedJournalSegment("Could not validate journal segment - hash did not match expected value - this journal segment is corrupted.");
                    }
                    bodySignature = fullJWS[0] + ".." + fullJWS[2]; // This is needed because nimbus-jose doesn't currently support verifying the hash itself - we've done this already though
                    parsedJWSObject = JWSObject.parse(bodySignature, new Payload(bodyHash));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new UnvalidatedJournalSegment("Could not validate journal segment - missing public key for origin {" + sourceInstance.toString() + "} - is this origin peered?");
            }

            if (publicJWK != null && parsedJWSObject != null) {
                try {
                    if (parsedJWSObject.verify(new Ed25519Verifier(publicJWK))) {
                        if (this.databaseInstance.getInstanceId() != sourceInstance) {
                            this.databaseInstance = null; // Make sure we don't get confused about what created this
                        }
                        return true;
                    } else {
                        throw new UnvalidatedJournalSegment("Could not validate journal segment - invalid signature for origin {" + sourceInstance.toString() + "} - is this origin peered correctly?");
                    }
                } catch (JOSEException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.databaseInstance = null; // Since this segment couldn't be verified, make sure we don't attach a database instance
        return false;
    }

    @Override
    public String toString() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        String[] segments = new String[this.segmentActions.size()];
        Queue<JournalEntry> originalQueue = new LinkedList<>();
        StringBuilder sb = null;
        int currentSegment = 0;
        while (this.segmentActions.isEmpty() == false) {
            JournalEntry currentEntry = segmentActions.poll();
            originalQueue.offer(currentEntry);
            sb = new StringBuilder();
            if (currentEntry instanceof NewNodeJournalEntry) {
                NewNodeJournalEntry newNodeEntry = ((NewNodeJournalEntry) currentEntry);
                sb.append("Entry-Type: new-node\r\n");
                sb.append("Global-Content-ID: "); sb.append(newNodeEntry.getGlobalId()); sb.append("\r\n");
                sb.append("Content-ID: "); sb.append(newNodeEntry.getId()); sb.append("\r\n");
                sb.append("Creation-Timestamp: "); sb.append(df.format(newNodeEntry.getCTime())); sb.append("\r\n");
                sb.append("Global-Creation-Timestamp: "); sb.append(df.format(newNodeEntry.getOCTime())); sb.append("\r\n");
                if (newNodeEntry.getCNodeId() != null) {
                    sb.append("Creator-ID: ");
                    sb.append(newNodeEntry.getCNodeId());
                    sb.append("\r\n");
                }
                if (newNodeEntry.getOCNodeId() != null) {
                    sb.append("Global-Creator-ID: ");
                    sb.append(newNodeEntry.getOCNodeId());
                    sb.append("\r\n");
                }
                if (newNodeEntry.getSchema() != null) {
                    sb.append("Schema: ");
                    sb.append(newNodeEntry.getSchema());
                    sb.append("\r\n");
                }

                sb.append("\r\n");
                String data = newNodeEntry.getData();
                if (data != null) {
                    sb.append(data);
                    sb.append("\r\n");
                }
            }
            segments[currentSegment] = sb.toString();
            currentSegment++;
        }

        this.segmentActions = originalQueue;

        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";
        sb = new StringBuilder(32);
        String boundaryString = null;
        String boundaryStringIncl = null;
        while (boundaryString == null) {
            for (int i = 0; i < 32; i++) {
                int index = (int) (alphaNumericString.length() * Math.random());
                sb.append(alphaNumericString.charAt(index));
            }
            boundaryString = sb.toString();
            boundaryStringIncl = "--" + boundaryString;
            for (int i = 0; i < segments.length; i++) {
                if (segments[i].contains(boundaryStringIncl)) {
                    boundaryString = null; // Try again, boundary string is somewhere in content!
                    break;
                }
            }
        }


        StringBuilder header = new StringBuilder();
        header.append("DDS-Journal-Segment-Version: 1.0\r\n");
        header.append("Original-Instance-ID: ");
        header.append(this.originalDatabaseInstanceId);
        header.append("\r\n");
        header.append("Segment-ID: ");
        header.append(this.getId());
        header.append("\r\n");
        header.append("Segment-Open-Timestamp: ");
        header.append(df.format(this.openDate));
        header.append("\r\n");
        header.append("Segment-Close-Timestamp: ");
        header.append(df.format(this.closeDate));
        header.append("\r\n");
        header.append("Content-Type: multipart/related; boundary=\"");
        header.append(boundaryString);
        header.append("\"\r\n");

        sb = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            sb.append(boundaryStringIncl);
            sb.append("\r\n");
            sb.append(segments[i]);
        }
        sb.append(boundaryStringIncl);
        sb.append("--\r\n");
        String bodyString = sb.toString();

        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bodyHash = digest.digest(bodyString.getBytes(StandardCharsets.UTF_8));
            header.append("Body-Signature: ");
            header.append(this.databaseInstance.signPayload(bodyHash));
            header.append("\r\n");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        header.append("\r\n");
        header.append(bodyString);
        return header.toString();
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
