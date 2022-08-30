package org.deegraph.conditions;

import org.deegraph.database.GraphDatabase;
import org.deegraph.database.Node;
import org.deegraph.database.NodePathContext;
import org.deegraph.database.SecurityContext;
import org.deegraph.formats.DataUrl;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedList;
import java.util.Locale;
import java.util.TimeZone;

public abstract class Condition {
    protected GraphDatabase graphDatabase;
    protected Condition(GraphDatabase graphDatabase) {
        this.graphDatabase = graphDatabase;
    }

    public static Condition fromComponents(GraphDatabase graphDatabase, LinkedList<String> components) {
        Condition returnCondition = null;

        /*System.out.print("CND [ ");
        for (String comp : components) {
            System.out.print(comp + " ");
        }
        System.out.println("]");*/

        if (components.size() == 1) {
            return new RawValue(graphDatabase, components.peek());
        }

        Condition leftCondition = null;
        String operator = null;
        LinkedList<String> sideComponents = new LinkedList<>();
        boolean invertSide = false;
        boolean leftSide = true;
        boolean escape = true;

        while (components.size() > 0) {
            sideComponents.add(components.poll());
            escape = false;
            while (!escape) {
                switch (sideComponents.peekLast()) {
                    case "NOT":
                    case "!":
                        sideComponents.removeLast(); // This is just a negate, and isn't useful to make another condition
                        sideComponents.add(components.poll());
                        invertSide = true;
                        break;
                    default:
                        escape = true;
                }
            }

            if (sideComponents.peekLast().equals("(")) { // We need to encapsulate this, there's an inner condition here
                sideComponents.removeLast();
                int bracketOpen = 1;
                while (components.size() > 0) {
                    sideComponents.add(components.poll());
                    if (sideComponents.peekLast().equals("(")) {
                        if (bracketOpen == 0) {
                            sideComponents.removeLast(); // Skip the first bracket, but not subsequent ones!
                        }
                        bracketOpen++;
                    } else if (sideComponents.peekLast().equals(")")) {
                        bracketOpen--;
                        if (bracketOpen == 0) {
                            sideComponents.removeLast(); // Skip the matching end bracket
                            break; // We've reached the end!, break out
                        }
                    }
                }
                returnCondition = Condition.fromComponents(graphDatabase, sideComponents);
                sideComponents = new LinkedList<>();
                sideComponents.add(components.poll()); // Load the next element
            } else {
                returnCondition = Condition.fromComponents(graphDatabase, sideComponents);
                sideComponents = new LinkedList<>();
                sideComponents.add(components.poll()); // Load the next element
            }

            if (operator != null) {
                switch (operator) {
                    case ">":
                        returnCondition = new GreaterThanCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "<":
                        returnCondition = new LessThanCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case ">=":
                        returnCondition = new GreaterThanOrEqualCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "<=":
                        returnCondition = new LessThanOrEqualCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "=":
                        returnCondition = new ImplicitEqualityCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "==":
                        returnCondition = new EqualityCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "===":
                        returnCondition = new IdentityCondition(graphDatabase, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "!=":
                        returnCondition = new LogicalNotCondition(graphDatabase, new ImplicitEqualityCondition(graphDatabase, leftCondition, returnCondition));
                        operator = null;
                        break;
                    case "!==":
                        returnCondition = new LogicalNotCondition(graphDatabase, new EqualityCondition(graphDatabase, leftCondition, returnCondition));
                        operator = null;
                        break;
                    case "ISNT":
                        returnCondition = new LogicalNotCondition(graphDatabase, new IdentityCondition(graphDatabase, leftCondition, returnCondition));
                        operator = null;
                        break;
                }
            }

            /*
                    case "&&":
                        returnCondition = new LogicalAndCondition(databaseInstance, leftCondition, returnCondition);
                        break;
                    case "||":
                        returnCondition = new LogicalOrCondition(databaseInstance, leftCondition, returnCondition);
                        break;
                    case "^|":
                        returnCondition = new LogicalXorCondition(databaseInstance, leftCondition, returnCondition);
                        break;
             */

            if (sideComponents.peekLast() == null) { // We've read a null, this means we're at the end of the condition
                sideComponents.removeLast();
            } else {
                String currentOperator = sideComponents.peekLast();
                currentOperator = (currentOperator == null) ? currentOperator : currentOperator.toUpperCase(Locale.ROOT);
                switch (currentOperator) {
                    case ">":
                    case "GT":
                    case "<":
                    case "LT":
                    case ">=":
                    case "GTE":
                    case "<=":
                    case "LTE":
                    case "=":
                    case "EQUALS":
                    case "==":
                    case "IDENTICAL":
                    case "===":
                    case "IS":
                    case "!=":
                    case "DIFFERENT":
                    case "!==":
                    case "ISNT":
                        sideComponents.removeLast(); // Get rid of the operator from the components
                        leftCondition = returnCondition;
                        break;
                    case "&&":
                    case "AND":
                    case "||":
                    case "OR":
                    case "^|":
                    case "XOR":
                        if (operator != null) {
                            switch (operator) {
                                case "&&":
                                    returnCondition = new LogicalAndCondition(graphDatabase, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                                case "||":
                                    returnCondition = new LogicalOrCondition(graphDatabase, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                                case "^|":
                                    returnCondition = new LogicalXorCondition(graphDatabase, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                            }
                        }
                        sideComponents.removeLast(); // Get rid of the operator from the components
                        returnCondition = invertSide ? new LogicalNotCondition(graphDatabase, returnCondition) : returnCondition; // Since this is a logical operater, apply any negation now
                        leftCondition = returnCondition;
                        invertSide = false; // Reset the invert flag
                        break;
                }
                switch (currentOperator) {
                    case ">":
                    case "GT":
                        operator = ">";
                        break;
                    case "<":
                    case "LT":
                        operator = "<";
                        break;
                    case ">=":
                    case "GTE":
                        operator = ">=";
                        break;
                    case "<=":
                    case "LTE":
                        operator = "<=";
                        break;
                    case "=":
                    case "EQUALS":
                        operator = "=";
                        break;
                    case "==":
                    case "IDENTICAL":
                        operator = "==";
                        break;
                    case "===":
                    case "IS":
                        operator = "===";
                        break;
                    case "!=":
                    case "DIFFERENT":
                        operator = "!=";
                        break;
                    case "!==":
                        operator = "!==";
                        break;
                    case "ISNT":
                        operator = "ISNT";
                        break;
                    case "&&":
                    case "AND":
                        operator = "&&";
                        break;
                    case "||":
                    case "OR":
                        operator = "||";
                        break;
                    case "^|":
                    case "XOR":
                        operator = "^|";
                        break;
                }
            }
        }

        if (operator != null) {
            switch (operator) {
                case "&&":
                    returnCondition = new LogicalAndCondition(graphDatabase, leftCondition, returnCondition);
                    break;
                case "||":
                    returnCondition = new LogicalOrCondition(graphDatabase, leftCondition, returnCondition);
                    break;
                case "^|":
                    returnCondition = new LogicalXorCondition(graphDatabase, leftCondition, returnCondition);
                    break;
            }
        }
        return invertSide ? new LogicalNotCondition(graphDatabase, returnCondition) : returnCondition;
    }

    public boolean eval(SecurityContext securityContext, NodePathContext context) {
        return coerceToBool(this.asLiteral(securityContext, context));
    }

    protected String metaProp(Node node, String key, NodePathContext context, Node requestingNode) throws ParseException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        switch (key) {
            case "@creator":
                if (node.getCNode() != null) {
                    String dataToParse = node.getCNode().getData(new SecurityContext(this.graphDatabase, requestingNode));
                    if (dataToParse != null) {
                        return new DataUrl(dataToParse).getStringData();
                    }
                }
                break;
            case "@original_creator_id":
                return "{" + node.getOCNodeId().toString() + "}";
            case "@id":
                return "{" + node.getId().toString() + "}";
            case "@original_id":
                return "{" + node.getOriginalId().toString() + "}";
            case "@original_instance_id":
                return "{" + node.getOriginalInstanceId().toString() + "}";
            case "@created":
                return df.format(node.getCTime());
            case "@originally_created":
                return df.format(node.getOCTime());
            case "@data":
                String dataToParse = node.getData(new SecurityContext(this.graphDatabase, requestingNode));
                if (dataToParse != null) {
                    return new DataUrl(dataToParse).getStringData();
                }
                break;
        }
        return null;
    }
    protected byte[] metaPropRaw(Node node, String key, NodePathContext context, Node requestingNode) throws ParseException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        switch (key) {
            case "@creator":
                if (node.getCNode() != null) {
                    String data = node.getCNode().getData(new SecurityContext(this.graphDatabase, requestingNode));
                    if (data != null) {
                        return data.getBytes(StandardCharsets.UTF_8);
                    }
                }
                break;
            case "@parsed_creator":
                if (node.getCNode() != null) {
                    String data = node.getCNode().getData(new SecurityContext(this.graphDatabase, requestingNode));
                    if (data != null) {
                        return new DataUrl(data).getStringData().getBytes(StandardCharsets.UTF_8);
                    }
                }
                break;
            case "@creator_id":
                if (node.getCNode() != null) {
                    if (node.getCNode().getId() != null) {
                        return ("{" + node.getCNode().getId().toString() + "}").getBytes(StandardCharsets.UTF_8);
                    }
                }
                break;
            case "@original_creator_id":
                return ("{" + node.getOCNodeId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@id":
                return ("{" + node.getId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@original_id":
                return ("{" + node.getOriginalId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@original_instance_id":
                return ("{" + node.getOriginalInstanceId().toString() + "}").getBytes(StandardCharsets.UTF_8);
            case "@created":
                return df.format(node.getCTime()).getBytes(StandardCharsets.UTF_8);
            case "@originally_created":
                return df.format(node.getOCTime()).getBytes(StandardCharsets.UTF_8);
            case "@data":
                String data = node.getData(new SecurityContext(this.graphDatabase, requestingNode));
                if (data != null) {
                    return data.getBytes(StandardCharsets.UTF_8);
                }
                break;
            case "@parsed_data":
                String dataToParse = node.getData(new SecurityContext(this.graphDatabase, requestingNode));
                if (dataToParse != null) {
                    return new DataUrl(dataToParse).getRawData();
                }
                break;
        }
        return null;
    }

    public String asLiteral(SecurityContext securityContext, NodePathContext context) {
        return this.eval(securityContext, context) ? "TRUE" : "FALSE";
    }

    protected static ValueTypes detectType(String strRepr) {
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

    protected static boolean coerceToBool(String strRepr) {
        if (strRepr == null) {
            throw new NumberFormatException();
        } else if (strRepr.equalsIgnoreCase("TRUE")) {
            return true;
        } else if (strRepr.equalsIgnoreCase("FALSE")) {
            return false;
        }
        return (coerceToNumber(strRepr) > 0.5d);
    }

    protected static double coerceToNumber(String strRepr) {
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
