package com.indentationerror.dds;

public class LogicalNotCondition extends LogicalCondition {
    private Condition condition;

    public LogicalNotCondition(DatabaseInstance databaseInstance, Condition condition) {
        super(databaseInstance);
        this.condition = condition;
    }

    @Override
    public boolean eval(NodePathContext context) {
        return !condition.eval(context);
    }
}
