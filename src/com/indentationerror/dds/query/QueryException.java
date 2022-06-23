package com.indentationerror.dds.query;

public class QueryException extends Exception {
    public QueryException(QueryExceptionCode code, int position) {
        super("Error {" + code + "} was thrown during query parse at position " + position);
    }

    public QueryException(QueryExceptionCode code, String info) {
        super("Error {" + code + "} was thrown during query parse at position, " + info);
    }

    public QueryException(QueryExceptionCode code) {
        super("Error {" + code + "} was thrown during query parse");
    }
}
