package com.indentationerror.dds;

public class LogicalOrCondition extends Condition {
    private Condition condition1;
    private Condition condition2;

    public LogicalOrCondition(DatabaseInstance databaseInstance, Condition condition1, Condition condition2) {
        super(databaseInstance);
        this.condition1 = condition1;
        this.condition2 = condition2;
    }

    @Override
    public boolean eval(NodePathContext context) {
        return condition1.eval(context) || condition2.eval(context);
    }
}
