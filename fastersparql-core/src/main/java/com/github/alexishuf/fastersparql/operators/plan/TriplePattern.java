package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.TripleRoleSet;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.TripleRoleSet.fromBitset;


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
        return fromBitset((s.isVar() ? 0x0 : 0x4) |
                          (p.isVar() ? 0x0 : 0x2) |
                          (o.isVar() ? 0x0 : 0x1));
    }

    public TripleRoleSet varRoles() {
        return fromBitset((s.isVar() ? 0x4 : 0x0) |
                          (p.isVar() ? 0x2 : 0x0) |
                          (o.isVar() ? 0x1 : 0x0));
    }


    @SuppressWarnings("unused")
    public TripleRoleSet groundRoles(Binding binding) {
        return fromBitset((!s.isVar() || binding.get(s) != null ? 0x4 : 0x0) |
                          (!p.isVar() || binding.get(p) != null ? 0x2 : 0x0) |
                          (!o.isVar() || binding.get(o) != null ? 0x1 : 0x0));
    }

    public TripleRoleSet varRoles(Binding binding) {
        return fromBitset((s.isVar() && binding.get(s) == null ? 0x4 : 0x0) |
                          (p.isVar() && binding.get(p) == null ? 0x2 : 0x0) |
                          (o.isVar() && binding.get(o) == null ? 0x1 : 0x0));
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new TriplePattern(s, p, o); }

    @Override
    public <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean canDedup) {
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
