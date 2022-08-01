package org.deegraph.database;

import org.deegraph.exceptions.DuplicateNodeStoreException;
import org.deegraph.exceptions.MissingNodeException;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public abstract class JournalEntry {
    protected Date timestamp;
    private static DateFormat date_format = null;
    public JournalEntry() {
        this.timestamp = new Date();
    }

    public JournalEntry(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public boolean replayOn(GraphDatabase graphDatabase, Node source) throws MissingNodeException, DuplicateNodeStoreException, ParseException {
        return false;
    }

    protected static String formatDate(Date date) {
        if (date_format == null) {
            TimeZone tz = TimeZone.getTimeZone("UTC");
            date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
            date_format.setTimeZone(tz);
        }
        return date_format.format(date);
    }

    protected static Date fromFormattedDate(String date) {
        TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(date);
        return Date.from(Instant.from(ta));
    }

    public static JournalEntry fromJson(JSONObject input) throws ParseException {
        if (input.getString("type") == null) {
            return null;
        }
        switch (input.getString("type").toUpperCase(Locale.ROOT)) {
            case "NEW_NODE":
                return NewNodeJournalEntry.fromJson(input);
            case "QUERY":
                return QueryJournalEntry.fromJson(input);
            case "TRUST_KEY":
                return TrustKeyJournalEntry.fromJson(input);
            default:
                return null;
        }
    }
    public JSONObject asJson() {
        return new JSONObject();
    }
}
