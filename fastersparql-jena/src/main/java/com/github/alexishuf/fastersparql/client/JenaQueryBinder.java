package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.JenaBatch;
import com.github.alexishuf.fastersparql.batch.type.JenaNodeParser;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.PooledTwoSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Query;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Var;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.syntax.syntaxtransform.QueryTransformOps;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract sealed class JenaQueryBinder extends AbstractOwned<JenaQueryBinder> {
    private static final int VAR_CACHE_MASK = 1023;
    static { assert Integer.bitCount(VAR_CACHE_MASK+1) == 1; }
    private static final Var[] VAR_CACHE = new Var[VAR_CACHE_MASK+1];
    private static final class Fac implements Supplier<JenaQueryBinder> {
        @Override public JenaQueryBinder get() {
            return new JenaQueryBinder.Concrete().takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "JenaQueryBinder.FAC";}
    }
    private static final Fac FAC = new Fac();
    private static final int BYTES = 16+2*4 + 10*(16+4+4);
    private static final Alloc<JenaQueryBinder> ALLOC = new Alloc<>(JenaQueryBinder.class,
            "JenaQueryBinder.ALLOC", Alloc.THREADS*32, FAC, BYTES);

    private final HashMap<Var, Node> var2node = new HashMap<>();
    private JenaNodeParser nodeParser = JenaNodeParser.create().takeOwnership(this);

    public static Orphan<JenaQueryBinder> create() {
        return ALLOC.create().releaseOwnership(RECYCLED);
    }

    private JenaQueryBinder() {}

    private static final class Concrete extends JenaQueryBinder
            implements Orphan<JenaQueryBinder> {
        @Override public JenaQueryBinder takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable JenaQueryBinder recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null)
            internalMarkGarbage(RECYCLED);
        return null;
    }

    @Override protected @Nullable JenaQueryBinder internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        nodeParser = Owned.safeRecycle(nodeParser, this);
        return null;
    }

    public Query bind(Query in, Binding binding) {
        var nodeParser = this.nodeParser != null ? this.nodeParser : createParser();
        if (!(binding instanceof BatchBinding bb && bb.batch instanceof JenaBatch)
                || !tryBindJena(bb)) {
            var2node.clear();
            try (var view = PooledTwoSegmentRope.ofEmpty()) {
                Vars vars = binding.vars();
                for (int i = 0, columns = vars.size(); i < columns; i++) {
                    var name = vars.get(i);
                    if (binding.get(i, view))
                        var2node.put(name2Var(name), nodeParser.makeNode(view));
                }
            }
        }
        return QueryTransformOps.transform(in, var2node);
    }

    private boolean tryBindJena(BatchBinding root) {
        var2node.clear();
        Vars vars = root.vars;
        for (int varIdx = 0, nVars = vars.size(); varIdx < nVars; varIdx++) {
            Batch<?> batch = Objects.requireNonNull(root.batch);
            BatchBinding bindingNode = root;
            int physCol = varIdx, physColsCount;
            while (physCol >= (physColsCount=batch.cols)) {
                physCol    -= physColsCount;
                bindingNode = Objects.requireNonNull(bindingNode.remainder);
                batch       = Objects.requireNonNull(bindingNode.batch);
            }
            if (batch instanceof JenaBatch jb)
                var2node.put(name2Var(vars.get(varIdx)), jb.obj(bindingNode.row, physCol));
            else
                return false;
        }
        return true;
    }
    

    private JenaNodeParser createParser() {
        return nodeParser = JenaNodeParser.create().takeOwnership(this);
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
