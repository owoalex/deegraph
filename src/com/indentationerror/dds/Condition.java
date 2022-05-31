package com.indentationerror.dds;

import java.util.LinkedList;
import java.util.Locale;

public class Condition {
    protected DatabaseInstance databaseInstance;
    protected Condition(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
    }
    public static Condition fromComponentsOld(DatabaseInstance databaseInstance, LinkedList<String> components) {
        Condition retCond = null;

        System.out.print("CND [ ");
        for (String comp : components) {
            System.out.print(comp + " ");
        }
        System.out.println("]");
        LinkedList<String> leftSideComponents = new LinkedList<>();

        int bracketOpen = 0;
        String current = components.poll();
        boolean invertLeft = false;
        switch (current.toUpperCase(Locale.ROOT)) {
            case "NOT":
            case "!":
                current = components.poll();
                invertLeft = true;
                break;
        }
        if (current.equals("(")) {
            bracketOpen++;
            while (components.size() > 0) {
                current = components.poll();
                if (current.equals("(")) {
                    if (bracketOpen != 0) {
                        leftSideComponents.add("("); // Skip the first bracket, but not subsequent ones!
                    }
                    bracketOpen++;
                } else if (current.equals(")")) {
                    bracketOpen--;
                    if (bracketOpen == 0) {
                        break; // We've reached the end!, break out
                    } else {
                        leftSideComponents.add(")");
                    }
                } else {
                    leftSideComponents.add(current);
                }
            }
        } else {
            leftSideComponents.add(current);
        }

        String operation = null;

        if (components.size() > 1) { // Check if there is at least and infix operator and a value on the right hand side
            current = components.poll();
            operation = current.toUpperCase(Locale.ROOT);

            boolean literalOperator = false;
            switch (operation) {
                case "!=":
                case "==":
                    literalOperator = true;
                    break;
            }

            LinkedList<String> rightSideComponents = new LinkedList<>();
            current = components.poll();

            boolean invertRight = false;

            switch (current.toUpperCase(Locale.ROOT)) {
                case "NOT":
                case "!":
                    current = components.poll();
                    invertRight = true;
                    break;
            }
            if (current.equals("(")) {
                bracketOpen++;
                while (components.size() > 0) {
                    current = components.poll();
                    if (current.equals("(")) {
                        if (bracketOpen != 0) {
                            rightSideComponents.add("("); // Skip the first bracket, but not subsequent ones!
                        }
                        bracketOpen++;
                    } else if (current.equals(")")) {
                        bracketOpen--;
                        if (bracketOpen == 0) {
                            break; // We've reached the end!, break out
                        } else {
                            rightSideComponents.add(")");
                        }
                    } else {
                        rightSideComponents.add(current);
                    }
                }
            } else {
                rightSideComponents.add(current);
            }

            if (literalOperator) {
                switch (operation) {
                    case "!=":
                        //retCond = new InvertCondition(databaseInstance, new EqualityCondition(databaseInstance, leftSideComponents.getFirst(), rightSideComponents.getFirst()));
                        break;
                    case "==":
                        //retCond = new EqualityCondition(databaseInstance, leftSideComponents.getFirst(), rightSideComponents.getFirst());
                        break;
                }
            } else {
                Condition condLeft = Condition.fromComponents(databaseInstance, leftSideComponents);
                if (invertLeft) {
                    condLeft = new LogicalNotCondition(databaseInstance, condLeft);
                }
                Condition condRight = Condition.fromComponents(databaseInstance, rightSideComponents);
                if (invertRight) {
                    condRight = new LogicalNotCondition(databaseInstance, condRight);
                }
                switch (operation) {
                    case "AND":
                    case "&&":
                        retCond = new LogicalAndCondition(databaseInstance, condLeft, condRight);
                        break;
                    case "XOR":
                        retCond = new LogicalXorCondition(databaseInstance, condLeft, condRight);
                        break;
                    case "OR":
                    case "||":
                        retCond = new LogicalOrCondition(databaseInstance, condLeft, condRight);
                        break;
                }
            }

            if (components.size() > 0) { // Now check if there's another operation chained on
                current = components.poll();
                operation = current.toUpperCase(Locale.ROOT);

                switch (operation) {
                    case "AND":
                    case "&&":
                        retCond = new LogicalAndCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
                        break;
                    case "XOR":
                        retCond = new LogicalXorCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
                        break;
                    case "OR":
                    case "||":
                        retCond = new LogicalOrCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
                        break;
                }

                return retCond;
            } else {

                return retCond;
            }
        }

        return null;
    }

    public static Condition fromComponents(DatabaseInstance databaseInstance, LinkedList<String> components) {
        Condition returnCondition = null;

        System.out.print("CND [ ");
        for (String comp : components) {
            System.out.print(comp + " ");
        }
        System.out.println("]");

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
