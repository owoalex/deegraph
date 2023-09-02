package org.deegraph.query;

public class QueryException extends Exception {
    protected QueryExceptionCode code;
    public QueryException(QueryExceptionCode code, int position) {
        super("Error {" + code + "} was thrown during query parse at position " + position);
        this.code = code;
    }

    public QueryException(QueryExceptionCode code, String info) {
        super("Error {" + code + "} was thrown during query parse at position, " + info);
        this.code = code;
    }

    public QueryExceptionCode getCode() {
        return this.code;
    }

    public QueryException(QueryExceptionCode code) {
        super("Error {" + code + "} was thrown during query parse");
        this.code = code;
    }
}
