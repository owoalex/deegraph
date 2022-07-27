package com.indentationerror.dds.database;

import com.indentationerror.dds.exceptions.DuplicateNodeStoreException;
import com.indentationerror.dds.exceptions.MissingNodeException;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

    public void replayOn(GraphDatabase graphDatabase) throws MissingNodeException, DuplicateNodeStoreException, ParseException {}

    protected static String formatDate(Date date) {
        if (date_format == null) {
            TimeZone tz = TimeZone.getTimeZone("UTC");
            date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
            date_format.setTimeZone(tz);
        }
        return date_format.format(date);
    }
    public JSONObject asJson() {
        return new JSONObject();
    }
}
