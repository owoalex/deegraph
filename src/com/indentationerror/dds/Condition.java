package com.indentationerror.dds;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Locale;

public class Condition {
    protected DatabaseInstance databaseInstance;
    protected Condition(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
    }
    public static Condition fromComponents(DatabaseInstance databaseInstance, LinkedList<String> components) {
        Condition retCond = null;

        System.out.print("CND [");
        for (String comp : components) {
            System.out.print(comp);
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
                    leftSideComponents.add("(");
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

        if (components.size() > 1) {
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
                        rightSideComponents.add("(");
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
                        retCond = new InvertCondition(databaseInstance, new EqualityCondition(databaseInstance, leftSideComponents.getFirst(), rightSideComponents.getFirst()));
                        break;
                    case "==":
                        retCond = new EqualityCondition(databaseInstance, leftSideComponents.getFirst(), rightSideComponents.getFirst());
                        break;
                }
            } else {
                Condition condLeft = Condition.fromComponents(databaseInstance, leftSideComponents);
                if (invertLeft) {
                    condLeft = new InvertCondition(databaseInstance, condLeft);
                }
                Condition condRight = Condition.fromComponents(databaseInstance, rightSideComponents);
                if (invertRight) {
                    condRight = new InvertCondition(databaseInstance, condRight);
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
        }

        // Now check if there's another operation chained on

        if (components.size() > 0) {
            current = components.poll();
            operation = current.toUpperCase(Locale.ROOT);

            switch (operation) {
                case "AND":
                case "&&":
                    return new LogicalAndCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
                case "XOR":
                    return new LogicalXorCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
                case "OR":
                case "||":
                    return new LogicalOrCondition(databaseInstance, retCond, Condition.fromComponents(databaseInstance, components));
            }

            return retCond;
        } else {
            return retCond;
        }
    }

    public boolean eval(NodePathContext context) {
        return false;
    }
}
