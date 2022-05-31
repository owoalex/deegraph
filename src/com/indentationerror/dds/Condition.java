package com.indentationerror.dds;

import java.util.LinkedList;
import java.util.Locale;

public class Condition {
    protected DatabaseInstance databaseInstance;
    protected Condition(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
    }

    public static Condition fromComponents(DatabaseInstance databaseInstance, LinkedList<String> components) {
        Condition returnCondition = null;

        /*System.out.print("CND [ ");
        for (String comp : components) {
            System.out.print(comp + " ");
        }
        System.out.println("]");*/

        if (components.size() == 1) {
            return new LiteralCondition(databaseInstance, components.peek());
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
                returnCondition = Condition.fromComponents(databaseInstance, sideComponents);
                sideComponents = new LinkedList<>();
                sideComponents.add(components.poll()); // Load the next element
            } else {
                returnCondition = Condition.fromComponents(databaseInstance, sideComponents);
                sideComponents = new LinkedList<>();
                sideComponents.add(components.poll()); // Load the next element
            }

            if (operator != null) {
                switch (operator) {
                    case "==":
                        returnCondition = new EqualityCondition(databaseInstance, leftCondition, returnCondition);
                        operator = null;
                        break;
                    case "!=":
                        returnCondition = new LogicalNotCondition(databaseInstance, new EqualityCondition(databaseInstance, leftCondition, returnCondition));
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
                    case "==":
                    case "EQUALS":
                    case "!=":
                    case "DIFFERENT":
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
                                    returnCondition = new LogicalAndCondition(databaseInstance, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                                case "||":
                                    returnCondition = new LogicalOrCondition(databaseInstance, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                                case "^|":
                                    returnCondition = new LogicalXorCondition(databaseInstance, leftCondition, returnCondition);
                                    operator = null;
                                    break;
                            }
                        }
                        sideComponents.removeLast(); // Get rid of the operator from the components
                        returnCondition = invertSide ? new LogicalNotCondition(databaseInstance, returnCondition) : returnCondition; // Since this is a logical operater, apply any negation now
                        leftCondition = returnCondition;
                        invertSide = false; // Reset the invert flag
                        break;
                }
                switch (currentOperator) {
                    case "==":
                    case "EQUALS":
                        operator = "==";
                        break;
                    case "!=":
                    case "DIFFERENT":
                        operator = "!=";
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
                    returnCondition = new LogicalAndCondition(databaseInstance, leftCondition, returnCondition);
                    break;
                case "||":
                    returnCondition = new LogicalOrCondition(databaseInstance, leftCondition, returnCondition);
                    break;
                case "^|":
                    returnCondition = new LogicalXorCondition(databaseInstance, leftCondition, returnCondition);
                    break;
            }
        }
        return invertSide ? new LogicalNotCondition(databaseInstance, returnCondition) : returnCondition;
    }

    public boolean eval(NodePathContext context) {
        try {
            if (this.asLiteral(context).toUpperCase(Locale.ROOT).equals("TRUE")) {
                return true;
            }
            return (Integer.valueOf(this.asLiteral(context)) > 0);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public String asLiteral(NodePathContext context) {
        return this.eval(context) ? "TRUE" : "FALSE";
    }
}
