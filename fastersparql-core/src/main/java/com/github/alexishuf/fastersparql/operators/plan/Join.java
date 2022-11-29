package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.sparql.parser.TriplePattern;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class Join<R, I> extends Plan<R, I>{
    public Join(List<? extends Plan<R, I>> operands, @Nullable Plan<R, I> unbound,
                @Nullable String name) {
        super(operands.get(0).rowType, operands, unbound, name);
    }


    @Override public void groupGraphPatternInner(StringBuilder out, int indent) {
        for (Plan<R, I> o : flatOperands()) {
            if (o instanceof TriplePattern<R,I> || o instanceof Values<R,I>)
                o.groupGraphPatternInner(out, indent);
            else
                o.groupGraphPattern(out, indent);
        }
    }

    @Override public BIt<R> execute(boolean canDedup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        unbound = unbound == null ? this.unbound : unbound;
        name = name == null ? this.name : name;
        return new Join<>(replacement, unbound, name);
    }
}
