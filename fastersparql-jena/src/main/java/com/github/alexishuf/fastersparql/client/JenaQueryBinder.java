package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Query;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Var;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.syntax.syntaxtransform.QueryTransformOps;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.util.NodeFactoryExtra;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.util.HashMap;

public class JenaQueryBinder {
    private static final int VAR_CACHE_MASK = 1023;
    static { assert Integer.bitCount(VAR_CACHE_MASK+1) == 1; }
    private static final Var[] VAR_CACHE = new Var[VAR_CACHE_MASK+1];

    private final HashMap<Var, Node> var2node = new HashMap<>();

    public Query bind(Query in, BatchBinding binding) {
        var2node.clear();
        try (var view = PooledTermView.ofEmptyString()) {
            Vars vars = binding.vars();
            for (int i = 0, columns = vars.size(); i < columns; i++) {
                var rope = vars.get(i);
                if (binding.get(i, view))
                    var2node.put(name2Var(rope), term2node(view));
            }
        }
        return QueryTransformOps.transform(in, var2node);
    }

    private static Node term2node(Term t) {
        // This is bad and if C2 does elide the 2 allocations and one byte[] copy
        // inside t.toString(), it will be REALLY BAD.
        return NodeFactoryExtra.parseNode(t.toString());
    }

    private static Var name2Var(SegmentRope name) {
        int bucket = name.hashCode()&VAR_CACHE_MASK;
        Var v = VAR_CACHE[bucket];
        if (v != null) {
            String varName = v.getVarName();
            try (var tmp = PooledMutableRope.getWithCapacity(varName.length()*4)) {
                tmp.append(varName);
                if (tmp.equals(name))
                    return v;
            }
        }
        VAR_CACHE[bucket] = v = Var.alloc(name.toString().intern());
        return v;
    }

}
