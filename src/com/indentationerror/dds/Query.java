package com.indentationerror.dds;

import java.util.*;

public class Query {
    public Query(String src) {
        StringBuilder inputWordBuffer = new StringBuilder();
        Stack<Character> inputString = new Stack<>();
        char[] inputArray = src.toCharArray();
        for (int i = inputArray.length - 1; i >= 0; i--) {
            inputString.push(inputArray[i]);
        }
        Queue<String> parsedQuery = new LinkedList<>();
        boolean quoteEscaped = false;

        while (inputString.size() > 0) {
            char currentChar = inputString.pop();
            if (quoteEscaped) {
                if (currentChar == '\\') {
                    currentChar = inputString.pop();
                    inputWordBuffer.append(currentChar);
                } else if (currentChar == '"') {
                    parsedQuery.add(inputWordBuffer.toString());
                    inputWordBuffer = new StringBuilder();
                    quoteEscaped = false;
                } else {
                    inputWordBuffer.append(currentChar);
                }
            } else {
                switch (currentChar) {
                    case '=':
                    case '>':
                    case '<':
                    case '+':
                    case '-':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        StringBuilder infix = new StringBuilder();
                        infix.append(currentChar);
                        boolean cancel = false;
                        while (!cancel) {
                            switch (inputString.peek()) {
                                case '=':
                                case '>':
                                case '<':
                                case '+':
                                case '-':
                                    infix.append(inputString.pop());
                                    break;
                                case ' ':
                                    inputString.pop();
                                    cancel = true;
                                    break;
                                default:
                                    cancel = true;
                            }
                        }
                        parsedQuery.add(infix.toString());
                        break;
                    case ',':
                    case ';':
                    case '(':
                    case ')':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        parsedQuery.add(String.valueOf(currentChar));
                        break;
                    case '"':
                        quoteEscaped = true;
                        break;
                    case ' ':
                        if (inputWordBuffer.length() > 0) {
                            parsedQuery.add(inputWordBuffer.toString());
                            inputWordBuffer = new StringBuilder();
                        }
                        break;
                    case '\\':
                        currentChar = inputString.pop();
                        inputWordBuffer.append(currentChar);
                        break;
                    default:
                        inputWordBuffer.append(currentChar);
                }
            }
        }

        parsedQuery.add(inputWordBuffer.toString());

        for (String str : parsedQuery) {
            System.out.println(str);
        }
    }

    public void runOn(DatabaseInstance databaseInstance) {

    }
}
