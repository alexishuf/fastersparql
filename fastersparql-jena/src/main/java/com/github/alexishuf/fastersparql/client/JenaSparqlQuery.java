package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Query;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Var;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.syntax.PatternVarsVisitor;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.syntax.syntaxtransform.QueryTransformOps;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlType;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;

public class JenaSparqlQuery implements SparqlQuery, SparqlType.SparqlGenerator {
    private final Query jenaQuery;
    private @MonotonicNonNull SegmentRope sparql;
    private @MonotonicNonNull Vars publicVars;
    private @MonotonicNonNull Vars allVars;

    public JenaSparqlQuery(Query jenaQuery) {
        this.jenaQuery = jenaQuery;
    }

    public Query jenaQuery() {return jenaQuery;}

    @Override public SparqlType sparqlType() {
        return SparqlType.SPARQL;
    }

    @Override public SegmentRope generateSparql() {
        if (sparql == null)
            sparql = FinalSegmentRope.asFinal(jenaQuery.toString());
        return sparql;
    }

    @Override public boolean isAsk() {return jenaQuery.isAskType();}

    @Override public boolean isGraph() {
        return jenaQuery.isConstructType() || jenaQuery.isDescribeType();
    }

    @Override public @Nullable DistinctType distinct() {
        if (jenaQuery.isDistinct()) return DistinctType.STRONG;
        if (jenaQuery.isReduced())  return DistinctType.REDUCED;
        return null;
    }

    @Override public Vars publicVars() {
        if (publicVars == null)
            publicVars = Vars.fromSet(jenaQuery.getResultVars());
        return publicVars;
    }

    private static void addTo(Vars.Mutable vars, @Nullable Collection<Var> nodes) {
        if (nodes != null) {
            for (Var v : nodes)
                vars.add(FinalSegmentRope.asFinal(v.getVarName()));
        }
    }

    @Override public Vars allVars() {
        if (allVars != null)
            return allVars;
        if (jenaQuery.isQueryResultStar())
            return allVars = publicVars();
        var vars = new Vars.Mutable(Math.max(10, 2*publicVars().size()));
        vars.addAll(publicVars);
        addTo(vars, jenaQuery.getValuesVariables());
        var patternVars = new ArrayList<Var>();
        jenaQuery.getQueryPattern().visit(new PatternVarsVisitor(patternVars));
        addTo(vars, patternVars);
        allVars = vars;
        return vars;
    }

    @Override public SparqlQuery toAsk() {
        if (isAsk())
            return this;
        Query q = QueryTransformOps.shallowCopy(jenaQuery);
        q.setQueryAskType();
        return new JenaSparqlQuery(q);
    }

    @Override public SparqlQuery toDistinct(DistinctType distinctType) {
        if (distinctType == DistinctType.STRONG && jenaQuery.isDistinct())
            return this;
        else if (jenaQuery.isReduced())
            return this;
        Query copy = QueryTransformOps.shallowCopy(jenaQuery);
        switch (distinctType) {
            case WEAK,REDUCED ->
                copy.setReduced(true);
            case STRONG ->
                copy.setDistinct(true);
        }
        return new JenaSparqlQuery(copy);
    }

    @Override public SparqlQuery bound(Binding binding) {
        var binder = JenaQueryBinder.create().takeOwnership(this);
        try {
            return new JenaSparqlQuery(binder.bind(jenaQuery, binding));
        } finally { binder.recycle(this); }
    }
}
