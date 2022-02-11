package com.github.alexishuf.fastersparql.operators.expressions;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter @Accessors(fluent = true)
public class UnboundVariablesException extends ExprCompilerException {
    private final String expression;
    private final List<String> unboundVars;

    public UnboundVariablesException(String expression, List<String> unboundVars) {
        super("No bindings for variables "+unboundVars+" in expression "+expression);
        this.expression = expression;
        this.unboundVars = unboundVars;
    }
}
