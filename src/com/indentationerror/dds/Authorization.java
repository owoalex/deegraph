package com.indentationerror.dds;

public class Authorization {
    private AuthorizationRule authorizingRule;
    private AuthorizedAction[] actionsAllowed;
    private AbsoluteNodePath[] targetNodes;

    public Authorization(AuthorizationRule basis) {
        this.authorizingRule = basis;
    }
}
