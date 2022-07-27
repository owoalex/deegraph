package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.ClosedJournalException;
import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import com.indentationerror.dds.exceptions.MissingNodeException;
import com.indentationerror.dds.exceptions.UnvalidatedJournalSegment;
import com.indentationerror.dds.query.Query;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.Ed25519Verifier;
import com.nimbusds.jose.jwk.JWK;
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
    private GraphDatabase graphDatabase;
    private UUID originalDatabaseInstanceId;

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
        this.localSegment = true;
    }

    public JournalSegment(GraphDatabase graphDatabaseBacking, File file) throws IOException, UnvalidatedJournalSegment {
        this.graphDatabase = graphDatabaseBacking;
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

    public void registerQuery(Query query) throws ClosedJournalException {
        if (this.open) {
            this.segmentActions.add(new QueryJournalEntry(query.toString(), query.getActor()));
            //System.out.println(query.toString());
        } else {
            throw new ClosedJournalException(this);
        }
    }

    public void replayOn(GraphDatabase graphDatabase, boolean faultTolerant) {
        for (JournalEntry journalEntry : this.segmentActions) {
            try {
                journalEntry.replayOn(graphDatabase);
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
                                    case "new-node": {
                                            System.out.println(new JSONObject(props));
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
                                            UUID oCNodeId = null;
                                            if (props.get("original-instance-id") != null) {
                                                oCNodeId = UUID.fromString(props.get("original-instance-id").get(0));
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
                                            TrustBlock trustRoot = null;
                                            if (props.get("trust-root") != null) {
                                                //trustRoot = props.get("trust-root").get(0);
                                            }
                                            String contentStr = null;
                                            if (content != null) {
                                                contentStr = String.join("\n", content.toArray(new String[0]));
                                            }

                                            NewNodeJournalEntry nodeJournalEntry = new NewNodeJournalEntry(localId, localId, this.graphDatabase.getInstanceId(), cNodeId, oCNodeId, contentStr, schema, creationDate, globalCreationDate, trustRoot);
                                            this.segmentActions.offer(nodeJournalEntry);
                                        }
                                        break;
                                    case "query": {
                                            Date ts = null;
                                            if (props.containsKey("timestamp") && props.get("timestamp").size() != 0) {
                                                String creationTimestamp = props.get("timestamp").get(0);
                                                TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(creationTimestamp);
                                                ts = Date.from(Instant.from(ta));
                                            }
                                            UUID actorId = null;
                                            if (props.get("actor-id") != null) {
                                                actorId = UUID.fromString(props.get("actor-id").get(0));
                                            }
                                            String contentStr = null;
                                            if (content != null) {
                                                contentStr = String.join("\n", content.toArray(new String[0]));
                                            }

                                            QueryJournalEntry queryJournalEntry = new QueryJournalEntry(contentStr, actorId);
                                            this.segmentActions.offer(queryJournalEntry);
                                        }
                                        break;
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Critical error processing journal segment " + this.journalUuid);
                            e.printStackTrace();
                        }
                        props = new HashMap<>(); // Clear the props now so new ones can be added on a blank slate
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

            File jwkFile = new File(this.graphDatabase.getDbLocation() + sourceInstance.toString() + ".public.jwk");

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
                        if (this.graphDatabase.getInstanceId() != sourceInstance) {
                            this.graphDatabase = null; // Make sure we don't get confused about what created this
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
        this.graphDatabase = null; // Since this segment couldn't be verified, make sure we don't attach a database instance
        return false;
    }

    public static JournalSegment fromDumpV2(GraphDatabase gdb, BufferedReader input) throws IOException, UnvalidatedJournalSegment {
        JournalSegment out = new JournalSegment(gdb);
        out.localSegment = false;
        return out;
    }


    public String toStringOld() {
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
                    sb.append(data.replaceAll("\r\n", "\n").replaceAll("\n", "\r\n")); // Normalise everything to \r\n
                    sb.append("\r\n");
                }
            }
            if (currentEntry instanceof QueryJournalEntry) {
                QueryJournalEntry specificEntry = ((QueryJournalEntry) currentEntry);
                sb.append("Entry-Type: query\r\n");
                sb.append("Actor-ID: "); sb.append(specificEntry.getActor()); sb.append("\r\n");
                sb.append("Timestamp: "); sb.append(df.format(specificEntry.getTimestamp())); sb.append("\r\n");

                sb.append("\r\n");
                String data = specificEntry.getQuery();
                if (data != null) {
                    sb.append(data.replaceAll("\r\n", "\n").replaceAll("\n", "\r\n")); // Normalise everything to \r\n
                    sb.append("\r\n");
                };
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
            header.append(this.graphDatabase.signPayload(bodyHash));
            header.append("\r\n");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        header.append("\r\n");
        header.append(bodyString);
        return header.toString();
    }

    @Override
    public String toString() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        //String[] segments = new String[this.segmentActions.size()];
        //Queue<JournalEntry> originalQueue = new LinkedList<>();
        StringBuilder sb = new StringBuilder();
        //int currentSegment = 0;
        JSONArray journalOutput = new JSONArray();

        while (this.segmentActions.isEmpty() == false) {
            JournalEntry currentEntry = segmentActions.poll();
            JSONObject entryJson = currentEntry.asJson();
            journalOutput.put(entryJson);
        }

        String stringJournal = journalOutput.toString(4).replaceAll("\r", "").replaceAll("\n", "\r\n");
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] journalAsBytes = stringJournal.getBytes(StandardCharsets.UTF_8);
        sb.append("deegraph-container-version: 1.0\r\n");
        sb.append("content-type: application/json\r\n");
        sb.append("content-signature: " + this.graphDatabase.signPayload(digest.digest(journalAsBytes)) + "\r\n");
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
