package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.TripleRoleSet;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.TripleRoleSet.fromBitset;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.VAR;


public final class TriplePattern extends Plan {
    public final Term s, p, o;

    public TriplePattern(Term s, Term p, Term o) {
        super(Operator.TRIPLE);
        this.s = s;
        this.p = p;
        this.o = o;
    }

    public TriplePattern(CharSequence s, CharSequence p, CharSequence o) {
        super(Operator.TRIPLE);
        this.s = Term.valueOf(s);
        this.p = Term.valueOf(p);
        this.o = Term.valueOf(o);
    }

    @SuppressWarnings("unused")
    public TripleRoleSet groundRoles() {
        return fromBitset((s.type() == VAR ? 0x0 : 0x4) |
                          (p.type() == VAR ? 0x0 : 0x2) |
                          (o.type() == VAR ? 0x0 : 0x1));
    }

    public TripleRoleSet freeRoles() {
        return fromBitset((s.type() == VAR ? 0x4 : 0x0) |
                          (p.type() == VAR ? 0x2 : 0x0) |
                          (o.type() == VAR ? 0x1 : 0x0));
    }


    @SuppressWarnings("unused")
    public TripleRoleSet groundRoles(Binding binding) {
        return fromBitset((s.type() != VAR || binding.has(s) ? 0x4 : 0x0) |
                          (p.type() != VAR || binding.has(p) ? 0x2 : 0x0) |
                          (o.type() != VAR || binding.has(o) ? 0x1 : 0x0));
    }

    public TripleRoleSet freeRoles(Binding binding) {
        return fromBitset((s.type() == VAR && !binding.has(s) ? 0x4 : 0x0) |
                          (p.type() == VAR && !binding.has(p) ? 0x2 : 0x0) |
                          (o.type() == VAR && !binding.has(o) ? 0x1 : 0x0));
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new TriplePattern(s, p, o); }

    @Override
    public <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean weakDedup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> type, boolean weakDedup) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof TriplePattern r
                && s.equals(r.s) && p.equals(r.p) && o.equals(r.o));
    }

    @Override public int hashCode() {
        return Objects.hash(s, p, o);
    }
}
