package com.indentationerror.dds;

public class InvertCondition extends Condition {
    private Condition condition;

    public InvertCondition(DatabaseInstance databaseInstance, Condition condition) {
        super(databaseInstance);
        this.condition = condition;
    }

    @Override
    public boolean eval(NodePathContext context) {
        return condition.eval(context);
    }
}
