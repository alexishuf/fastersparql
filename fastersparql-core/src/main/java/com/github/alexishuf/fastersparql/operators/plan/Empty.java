package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class Empty extends Plan {
    public static final Empty EMPTY = new Empty(null, null);

    public Empty(Plan other) {
        super(Operator.EMPTY);
        this.allVars = other.allVars();
        this.publicVars = other.publicVars();
    }

    public Empty(@Nullable Vars publicVars, @Nullable Vars allVars) {
        super(Operator.EMPTY);
        this.allVars    = allVars    == null ? Vars.EMPTY : allVars;
        this.publicVars = publicVars == null ? Vars.EMPTY : publicVars;
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new Empty(publicVars, allVars); }

    @Override
    public <B extends Batch<B>>
    BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean canDedup) {
        Vars vars = publicVars;
        if (binding != null && vars.intersects(binding.vars))
            vars = vars.minus(binding.vars);
        return new EmptyBIt<>(bt, vars);
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof Empty r
                && Objects.equals(publicVars, r.publicVars)
                && Objects.equals(allVars   , r.allVars   );
    }

    @Override public int hashCode() {
        return Objects.hash(type, publicVars, allVars);
    }
}
