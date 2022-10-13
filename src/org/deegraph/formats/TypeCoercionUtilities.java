package org.deegraph.formats;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class TypeCoercionUtilities {
    public static ValueTypes detectType(String strRepr) {
        if (strRepr.equalsIgnoreCase("TRUE") || strRepr.equalsIgnoreCase("FALSE")) {
            return ValueTypes.BOOL;
        } else if (strRepr.matches("^[0-9]+(\\.[0-9]+)?$")) {
            return ValueTypes.NUMBER;
        } else if (strRepr.matches("^0x[0-9a-fA-F]+$")) {
            return ValueTypes.NUMBER;
        } else if (strRepr.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(\\.[0-9]+)?([-+][0-9]{2}(:[0-9]{2})?|Z)$")) {
            return ValueTypes.NUMBER;
        }
        return ValueTypes.STRING;
    }

    public static boolean coerceToBool(String strRepr) {
        if (strRepr == null) {
            throw new NumberFormatException();
        } else if (strRepr.equalsIgnoreCase("TRUE")) {
            return true;
        } else if (strRepr.equalsIgnoreCase("FALSE")) {
            return false;
        }
        return (coerceToNumber(strRepr) > 0.5d);
    }

    public static double coerceToNumber(String strRepr) {
        if (strRepr == null) {
            throw new NumberFormatException();
        } else if (strRepr.equalsIgnoreCase("TRUE")) {
            return 1;
        } else if (strRepr.equalsIgnoreCase("FALSE")) {
            return 0;
        } else if (strRepr.matches("^[0-9]+(\\.[0-9]+)?$")) {
            return Double.parseDouble(strRepr);
        } else if (strRepr.matches("^0x[0-9a-fA-F]+$")) {
            return Long.parseLong(strRepr.substring(2), 16);
        } else if (strRepr.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(\\.[0-9]+)?Z$")) {
            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(strRepr);
            Instant i = Instant.from(ta);
            return i.toEpochMilli() / 1000.0d;
        } else if (strRepr.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2})?(\\.[0-9]+)?[-+][0-9]{2}(:[0-9]{2})?$")) {
            TemporalAccessor ta = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(strRepr);
            Instant i = Instant.from(ta);
            return i.toEpochMilli() / 1000.0d;
        }
        throw new NumberFormatException();
    }
}
