package com.github.alexishuf.fastersparql.operators.expressions;

import java.util.List;

public class UnboundVariablesException extends ExprCompilerException {
    private final String expression;
    private final List<String> unboundVars;

    public UnboundVariablesException(String expression, List<String> unboundVars) {
        super("No bindings for variables "+unboundVars+" in expression "+expression);
        this.expression = expression;
        this.unboundVars = unboundVars;
    }

    @SuppressWarnings("unused")
    public String expression() { return expression; }
    @SuppressWarnings("unused")
    public List<String> unboundVars() { return unboundVars; }
}
