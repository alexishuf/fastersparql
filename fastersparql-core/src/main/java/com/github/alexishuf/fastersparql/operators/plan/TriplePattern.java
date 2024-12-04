package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.TripleRoleSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.TripleRoleSet.fromBitset;


public final class TriplePattern extends Plan {
    public final Term s, p, o;
    public final Vars vars;

    public TriplePattern(Term s, Term p, Term o) {
        super(Operator.TRIPLE);
        this.s = s;
        this.p = p;
        this.o = o;
        this.vars = initVars();
    }

    public TriplePattern(CharSequence s, CharSequence p, CharSequence o) {
        super(Operator.TRIPLE);
        this.s = Term.valueOf(s);
        this.p = Term.valueOf(p);
        this.o = Term.valueOf(o);
        this.vars = initVars();
    }

    private Vars initVars() {
        if (s.isGround() && p.isGround() && o.isGround()) return Vars.EMPTY;
        var vars = new Vars.Mutable(3);
        if (s.isVar()) vars.add(s);
        if (p.isVar()) vars.add(p);
        if (o.isVar()) vars.add(o);
        return vars;
    }

    @SuppressWarnings("unused")
    public TripleRoleSet groundRoles() {
        return fromBitset((s.isVar() ? 0x0 : 0x4) |
                          (p.isVar() ? 0x0 : 0x2) |
                          (o.isVar() ? 0x0 : 0x1));
    }

    public TripleRoleSet freeRoles() {
        return fromBitset((s.isVar() ? 0x4 : 0x0) |
                          (p.isVar() ? 0x2 : 0x0) |
                          (o.isVar() ? 0x1 : 0x0));
    }


    @SuppressWarnings("unused")
    public TripleRoleSet groundRoles(Binding binding) {
        return fromBitset((s.isGround() || binding.has(s) ? 0x4 : 0x0) |
                          (p.isGround() || binding.has(p) ? 0x2 : 0x0) |
                          (o.isGround() || binding.has(o) ? 0x1 : 0x0));
    }

    public TripleRoleSet freeRoles(Binding binding) {
        return fromBitset((s.isVar() && !binding.has(s) ? 0x4 : 0x0) |
                          (p.isVar() && !binding.has(p) ? 0x2 : 0x0) |
                          (o.isVar() && !binding.has(o) ? 0x1 : 0x0));
    }

    public TripleRoleSet dummyRoles(@Nullable Binding b) {
        int bitset;
        if (b == null) {
            bitset = (s == Term.GROUND ? 4 : 0)
                   | (p == Term.GROUND ? 2 : 0)
                   | (o == Term.GROUND ? 1 : 0);
        } else {
            bitset = (s == Term.GROUND || b.hasSpecialRef(s, Term.GROUND) ? 4 : 0)
                   | (p == Term.GROUND || b.hasSpecialRef(p, Term.GROUND) ? 2 : 0)
                   | (o == Term.GROUND || b.hasSpecialRef(o, Term.GROUND) ? 1 : 0);
        }
        return TripleRoleSet.fromBitset(bitset);
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new TriplePattern(s, p, o); }

    @Override
    public <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean weakDedup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> type, Vars rebindHint, boolean weakDedup) {
        throw new UnsupportedOperationException();
    }

    @Override public String journalName() {return toString();}

    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof TriplePattern r
                && s.equals(r.s) && p.equals(r.p) && o.equals(r.o));
    }

    @Override public int hashCode() {
        return Objects.hash(s, p, o);
    }
}
