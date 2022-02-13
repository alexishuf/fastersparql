package com.github.alexishuf.fastersparql.jena.operators.expressions;

import com.github.alexishuf.fastersparql.jena.JenaUtils;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluator;
import com.github.alexishuf.fastersparql.operators.expressions.UnboundVariablesException;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.jena.atlas.io.StringWriterI;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.VariableNotBoundException;
import org.apache.jena.sparql.function.FunctionEnvBase;
import org.apache.jena.sparql.util.Context;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JenaExprEvaluator<R> implements ExprEvaluator<R> {
    private static final Pattern VAR = Pattern.compile(" \\?([^ ,?]+)");
    private static final NodeFormatterNT formatter = new NodeFormatterNT();
    private static final DatasetGraph dsg = DatasetFactory.create().asDatasetGraph();
    private static final Context jCtx = Context.setupContextForDataset(ARQ.getContext(), dsg);
    private static final FunctionEnvBase fnEnv = new FunctionEnvBase(jCtx, dsg.getDefaultGraph(), dsg);

    private final String exprString;
    private final Expr expr;
    private final RowBinding binding;


    public JenaExprEvaluator(String exprString, Expr expr, RowOperations rowOperations, List<String> varNames) {
        this.exprString = exprString;
        this.expr = expr;
        this.binding = new RowBinding(rowOperations, varNames);
    }

    @Override public String evaluate(R row) {
        NodeValue value;
        try {
            value = expr.eval(binding.setValues(row), fnEnv);
        } catch (VariableNotBoundException e) {
            List<String> unbound = new ArrayList<>();
            for (Matcher m = VAR.matcher(e.getMessage()); m.find(); )
                unbound.add(m.group(1));
            throw new UnboundVariablesException(exprString, unbound);
        }
        StringWriterI writer = new StringWriterI();
        formatter.format(writer, value.asNode());
        return writer.toString();
    }

    @EqualsAndHashCode @ToString
    private static class RowBinding implements Binding {
        private final RowOperations ro;
        private final List<Var> vars;
        private final Map<Var, Integer> var2idx;
        private final Node[] values;

        public RowBinding(RowOperations ro, List<String> varNames) {
            int size = varNames.size();
            this.ro      = ro;
            this.vars    = new ArrayList<>(size);
            this.values  = new Node[size];
            this.var2idx = new HashMap<>(size *2);
            for (int i = 0; i < size; i++) {
                Var v = Var.alloc(varNames.get(i));
                vars.add(v);
                var2idx.put(v, i);
            }
        }

        RowBinding setValues(Object row) {
            for (int i = 0, size = vars.size(); i < size; i++) {
                String name = vars.get(i).getVarName();
                values[i] = JenaUtils.fromNT(ro.getNT(row, i, name));
            }
            return this;
        }

        @Override public Iterator<Var> vars()            { return vars.iterator(); }
        @Override public boolean       contains(Var var) { return vars.contains(var); }
        @Override public int           size()            { return vars.size(); }
        @Override public boolean       isEmpty()         { return vars.isEmpty(); }

        @Override public void forEach(BiConsumer<Var, Node> action) {
            for (int i = 0, size = size(); i < size; i++)
                action.accept(vars.get(i), values[i]);
        }

        @Override public Node get(Var var) {
            int i = var2idx.getOrDefault(var, -1);
            return i < 0 ? null : values[i];
        }
    }
}
