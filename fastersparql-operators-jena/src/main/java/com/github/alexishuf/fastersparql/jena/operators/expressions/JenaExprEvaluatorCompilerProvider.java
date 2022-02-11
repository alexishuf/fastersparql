package com.github.alexishuf.fastersparql.jena.operators.expressions;

import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompiler;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompilerProvider;
import org.apache.jena.sys.JenaSystem;

public class JenaExprEvaluatorCompilerProvider implements ExprEvaluatorCompilerProvider {
    @Override public ExprEvaluatorCompiler get() {
        JenaSystem.init();
        return JenaExprEvaluatorCompiler.INSTANCE;
    }
    @Override public int    order() { return 100; }
    @Override public String  name() { return "jena"; }
}
