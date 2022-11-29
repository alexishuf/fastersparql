package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.isVar;

public class TriplePattern<R, I> extends Plan<R, I> {
    public final I s, p, o;

    public TriplePattern(RowType<R, I> rowType, I s, I p, I o) {
        super(rowType, List.of(), null, null);
        this.s = s;
        this.p = p;
        this.o = o;
    }

    @Override public BIt<R> execute(boolean canDedup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement,
                           @Nullable Plan<R, I> unbound,
                           @Nullable String name) {
        return this;
    }

    private I bind(I term, Binding binding) {
        String nt = rowType.toNT(term);
        if (isVar(nt, 0, -1)) {
            int i = binding.vars.indexOf(nt, 1, nt.length());
            if (i >= 0) {
                nt = binding.get(i);
                if (nt != null)
                    return rowType.fromNT(nt, 0, nt.length());
            }
        }
        return term;
    }

    @Override public Plan<R, I> bind(Binding binding) {
        I s = bind(this.s, binding), p = bind(this.p, binding), o = bind(this.o, binding);
        if (s != this.s || p != this.p || o != this.o)
            return new TriplePattern<>(rowType, s, p, o);
        return this;
    }

    @Override protected Vars computeVars(boolean all) {
        var vars = new Vars.Mutable(3);
        String nt;
        if (isVar((nt = rowType.toNT(s)), 0, -1)) vars.add(nt.substring(1));
        if (isVar((nt = rowType.toNT(p)), 0, -1)) vars.add(nt.substring(1));
        if (isVar((nt = rowType.toNT(o)), 0, -1)) vars.add(nt.substring(1));
        return vars.isEmpty() ? Vars.EMPTY : vars;
    }

    @Override public String algebraName() {
        return rowType.toSparql(s)+' '+rowType.toSparql(p)+' '+rowType.toSparql(o);
    }

    @Override public void groupGraphPatternInner(StringBuilder out, int indent) {
        newline(out, indent);
        out.append(rowType.toSparql(s)).append(' ');
        out.append(rowType.toSparql(p)).append(' ');
        out.append(rowType.toSparql(o)).append(" .");
    }


}
