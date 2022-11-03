package com.github.alexishuf.fastersparql.jena.operators.expressions;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluator;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompiler;
import com.github.alexishuf.fastersparql.operators.expressions.ExprSyntaxException;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import java.util.List;

public class JenaExprEvaluatorCompiler implements ExprEvaluatorCompiler {
    public static final JenaExprEvaluatorCompiler INSTANCE = new JenaExprEvaluatorCompiler();
    private static final String HOST_SPARQL =  "PREFIX xsd: <"+ XSD.NS+">\n"+
            "PREFIX rdf: <"+ RDF.getURI()+">\n" +
            "PREFIX rdfs: <"+ RDFS.getURI()+">\n" +
            "PREFIX owl: <"+ OWL2.NS+">\n" +
            "SELECT * WHERE {\n";

    @Override
    public <R> ExprEvaluator<R> compile(RowType<R, ?> rowType, List<String> rowVarNames,
                                        CharSequence inExpr) {
        String expr = inExpr.toString();
        StringBuilder b = new StringBuilder(HOST_SPARQL.length() + expr.length() + 16);
        b.append(HOST_SPARQL).append("FILTER(").append(expr).append(")}");
        Query query;
        try {
            query = QueryFactory.create(b.toString());
        } catch (QueryException e) {
            throw new ExprSyntaxException(expr, e.getMessage());
        }
        Expr[] out = {null};
        query.getQueryPattern().visit(new ElementVisitorBase() {
            @Override public void visit(ElementFilter el) {
                out[0] = el.getExpr();
            }
            @Override public void visit(ElementGroup el) {
                for (Element e : el.getElements()) e.visit(this);
            }
        });
        if (out[0] == null)
            throw new ExprSyntaxException(expr, "no expression found");
        return new JenaExprEvaluator<>(expr, out[0], rowType, rowVarNames);
    }
}
