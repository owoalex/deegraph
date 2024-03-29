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

import static org.deegraph.formats.TypeCoercionUtilities.coerceToBool;

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



    public String asLiteral(SecurityContext securityContext, NodePathContext context) {
        return this.eval(securityContext, context) ? "TRUE" : "FALSE";
    }


}
