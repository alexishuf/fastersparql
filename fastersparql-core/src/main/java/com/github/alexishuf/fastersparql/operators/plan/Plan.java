package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.MetricsStage;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/** Represents a tree of operators applied to their arguments */
public abstract sealed class Plan implements SparqlQuery
        permits Join, Union, LeftJoin, Minus, Exists, Modifier, Empty, Query, TriplePattern, Values {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    public final Operator type;
    protected @MonotonicNonNull Vars publicVars, allVars;
    public @Nullable Plan left, right;
    public Plan @Nullable [] operandsArray;
    private List<MetricsListener> listeners = List.of();
    private int id = 0;

    public Plan(Operator type) {
        this.type = type;
    }

    public int id() { return id == 0 ? id = nextId.getAndIncrement() : id; }

    private static final ByteRope EMPTY_NM = new ByteRope("Empty");
    private static final ByteRope EXISTS_NM = new ByteRope("Exists");
    private static final ByteRope NOT_EXISTS_NM = new ByteRope("NotExists");
    private static final ByteRope JOIN_NM = new ByteRope("Join");
    private static final ByteRope LEFT_JOIN_NM = new ByteRope("LeftJoin");
    private static final ByteRope MINUS_NM = new ByteRope("Minus");
    private static final ByteRope UNION_NM = new ByteRope("Union");
    private static final ByteRope UNION_NM_CD = new ByteRope("Union[crossDedup]");
    private static final ByteRope VALUES_NM = new ByteRope("Values");
    private static final byte[] QUERY_LBRAC = "Query[".getBytes(UTF_8);
    /** Operator name to be used in {@link Plan#toString()} */
    public Rope algebraName() {
        return switch (type) {
            case JOIN       -> JOIN_NM;
            case LEFT_JOIN  -> LEFT_JOIN_NM;
            case MINUS      -> MINUS_NM;
            case EMPTY      -> EMPTY_NM;
            case EXISTS     -> EXISTS_NM;
            case NOT_EXISTS -> NOT_EXISTS_NM;
            case VALUES     -> new ByteRope().append(VALUES_NM).append(allVars());
            case UNION -> ((Union)this).crossDedup ? UNION_NM : UNION_NM_CD;
            case QUERY -> {
                Query q = (Query) this;
                ByteRope rb = new ByteRope(256);
                rb.append(QUERY_LBRAC).append(q.client.endpoint().uri()).append(']').append('(');
                Rope sparql = sparql();
                yield  sparql.len() < 80
                        ? rb.appendEscapingLF(sparql).append(')')
                        : rb.append('\n').indented(2, sparql).append('\n').append(')');
            }
            case TRIPLE -> {
                TriplePattern t = (TriplePattern) this;
                ByteRope r = new ByteRope();
                t.s.toSparql(r,             PrefixAssigner.NOP);
                t.p.toSparql(r.append(' '), PrefixAssigner.NOP);
                t.o.toSparql(r.append(' '), PrefixAssigner.NOP);
                r.append(' ');
                yield r;
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    private static final ByteRope UNION_SP = new ByteRope(" UNION");
    private static final ByteRope MINUS_SP = new ByteRope("MINUS");
    private static final ByteRope LEFT_JOIN_SP = new ByteRope("OPTIONAL");
    private static final ByteRope EXISTS_SP = new ByteRope("FILTER EXISTS");
    private static final ByteRope NOT_EXISTS_SP = new ByteRope("FILTER NOT EXISTS");
    private static final ByteRope FILTER_SP = new ByteRope("FILTER");
    public Rope sparqlName() {
        return switch (type) {
            case UNION -> UNION_SP;
            case MINUS -> MINUS_SP;
            case EXISTS -> EXISTS_SP;
            case NOT_EXISTS -> NOT_EXISTS_SP;
            case LEFT_JOIN -> LEFT_JOIN_SP;
            default -> ByteRope.EMPTY;
        };
    }

    /** How many operands this {@link Plan} has directly */
    public final int opCount() {
        return operandsArray == null ? right == null ? left == null ? 0 : 1 : 2 : operandsArray.length;
    }

    /** Get the {@code i}-th direct operand of this Plan. See {@link Plan#opCount()}. */
    public final Plan op(int i) {
        if      (operandsArray != null) return operandsArray[i];
        else if (i             ==    0) return left;
        else if (i             ==    1) return right;
        throw new IndexOutOfBoundsException();
    }

    public final Plan  left() { return  left; }
    public final Plan right() { return right; }

    /** Replace the {@code i}-th operand of this {@link Plan} with {@code replacement}. */
    public final void replace(int i, Plan replacement) {
        if (operandsArray != null)
            operandsArray[i] = replacement;
        if      (i == 0) left = replacement;
        else if (i == 1) right = replacement;
    }

    public final void replace(int begin, int end, Plan replacement) {
        Plan[] currentArray = operandsArray;
        if (currentArray != null) {
            Plan[] replacementArray = new Plan[currentArray.length - (end - begin) + 1];
            int dst = 0;
            for (int i = 0; i < begin; i++)
                replacementArray[dst++] = currentArray[i];
            replacementArray[dst++] = replacement;
            for (int i = end; i < currentArray.length; i++)
                replacementArray[dst++] = currentArray[i];
            operandsArray = replacementArray;
            left = replacementArray[0];
            if (replacementArray.length > 1)
                right = replacementArray[0];
        } else {
            if (begin == 0) {
                if (end > 0) left = replacement;
                if (end > 1) right = null;
            } else if (begin == 1 && end > 1) {
                right = replacement;
            }
        }
    }

    /** Replace all operands of this {@link Plan} with the given {@code operands}. */
    public final void replace(Plan[] operands) {
        if (operands.length < 3) {
            operandsArray = null;
            left  = operands.length > 0 ? operands[0] : null;
            right = operands.length > 1 ? operands[1] : null;
        } else {
            left  = operands[0];
            right = operands[1];
            operandsArray = operands;
        }
    }

    @Override public SegmentRope sparql() {
        if (this instanceof Query q)
            return q.sparql.sparql();
        ByteRope rb = new ByteRope(256);
        rb.append(SparqlSkip.SELECT_u8).append(' ').append('*');
        groupGraphPattern(rb, 0, PrefixAssigner.NOP);
        return rb;
    }

    /**
     * Write this plan as a {@code GroupGraphPattern} production from the SPARQL grammar
     *
     * @param out where to write the SPARQL to.
     */
    public final void groupGraphPattern(ByteSink<?, ?> out, int indent, PrefixAssigner assigner) {
        out.newline(indent++).append('{');
        groupGraphPatternInner(out, indent, assigner);
        out.newline(--indent).append('}');
    }

    protected final void groupGraphPatternInnerOp(ByteSink<?, ?> out, int indent, PrefixAssigner assigner) {
        switch (type) {
            case JOIN,TRIPLE,VALUES -> groupGraphPatternInner(out, indent, assigner);
            default                 -> groupGraphPattern(out, indent, assigner);
        }
    }

    /**
     * Equivalent to {@code groupGraphPattern(out, indent)} without the surrounding
     * {@code '{'} and {@code'}'}.
     */
    public void groupGraphPatternInner(ByteSink<?, ?> out, int indent, PrefixAssigner assigner) {
        switch (type) {
            case JOIN -> {
                for (int i = 0, n = opCount(); i < n; i++)
                    op(i).groupGraphPatternInnerOp(out, indent, assigner);
            }
            case UNION -> {
                for (int i = 0, n = opCount(); i < n; i++)
                    op(i).groupGraphPattern(i>0 ? out.append(UNION_SP) : out, indent, assigner);
            }
            case QUERY -> {
                SparqlQuery q = ((Query) this).sparql;
                if (q instanceof Plan p) p.groupGraphPatternInner(out, indent, assigner);
                else                     out.indented(indent, q.sparql());
            }
            case TRIPLE -> {
                var p = (TriplePattern) this;
                p.s.toSparql(out.newline(indent), assigner);
                p.p.toSparql(out.append(' '), assigner);
                p.o.toSparql(out.append(' '), assigner);
                out.append(' ').append('.');
            }
            case EMPTY -> {}
            default -> {
                ArrayDeque<Plan> stack = new ArrayDeque<>();
                for (Plan p = this; p != null; ) {
                    stack.push(p);
                    p = switch (p.type) {
                        case LEFT_JOIN, MINUS, EXISTS, NOT_EXISTS, MODIFIER -> p.left;
                        default -> {p.groupGraphPatternInnerOp(out, indent, assigner); yield null;}
                    };
                }
                for (Plan o; (o = stack.pollFirst()) != null; ) {
                    switch (o.type) {
                        case EXISTS,NOT_EXISTS,MINUS,LEFT_JOIN -> {
                            out.newline(indent).append(o.sparqlName());
                            requireNonNull(o.right).groupGraphPattern(out, indent, assigner);
                        }
                        case MODIFIER -> {
                            for (Expr e : ((Modifier)o).filters) {
                                out.newline(indent);
                                if (e instanceof Expr.Exists ex) {
                                    out.append(ex.negate() ? NOT_EXISTS_SP : EXISTS_SP);
                                    ex.filter().groupGraphPattern(out, indent, assigner);
                                } else {
                                    e.toSparql(out.append(FILTER_SP), assigner);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override public boolean isAsk() {
        return this instanceof Modifier m
                && m.offset == 0 && m.limit == 1 && m.publicVars().isEmpty();
    }

    /** This algebra does not support graph queries */
    @Override public boolean isGraph() { return false; }

    /**
     * The would-be value of {@link BIt#vars()} upon {@link Plan#execute(BatchType, boolean)}.
     *
     * @return a non-null (but possibly empty) list of non-null and non-empty variable names
     *         (i.e., no leading {@code ?} or {@code $}).
     */
    @Override public final Vars publicVars() {
        if (publicVars == null) publicVars = computePublicVars();
        return publicVars;
    }
    private Vars computePublicVars() {
        return switch (type) {
            case MODIFIER -> {
                Vars projection = ((Modifier) this).projection;
                yield projection == null ? requireNonNull(left).publicVars() : projection;
            }
            case JOIN -> {
                Vars projection = ((Join) this).projection;
                yield projection == null ? publicVarsUnion() : projection;
            }
            case QUERY -> ((Query)this).sparql.publicVars();
            case TRIPLE -> allVars();
            case EXISTS,NOT_EXISTS,MINUS -> requireNonNull(left).publicVars();
            default -> publicVarsUnion();
        };
    }

    private Vars publicVarsUnion() {
        if (operandsArray != null)
            return publicVars(operandsArray);
        if (right == null)
            return left == null ? Vars.EMPTY : left.publicVars();
        return requireNonNull(left).publicVars().union(right.publicVars());
    }

    public static Vars publicVars(Plan... operands) {
        Vars union = new Vars.Mutable(10);
        for (Plan o : operands) union.addAll(o.publicVars());
        return union;
    }

    /**
     * All vars used within this plan, not only those exposed in results.
     *
     * <p>This is the list of variables that should be used with {@link Plan#bound(Binding)} and related
     * methods.</p>
     *
     * @return a non-null (possibly empty) list of non-null and non-empty variable names
     *         (i.e., no preceding {@code ?} or {@code $}).
     */
    @Override public final Vars allVars() {
        if (allVars == null) allVars = computeAllVars();
        return allVars;
    }
    private Vars computeAllVars() {
        return switch (type) {
            case TRIPLE -> ((TriplePattern)this).vars;
            case QUERY -> ((Query)this).sparql.allVars();
            case MODIFIER -> {
                //noinspection DataFlowIssue
                Vars leftAllVars = left.allVars(), vars = leftAllVars;
                Modifier m = (Modifier) this;
                if (m.projection != null)
                    vars = m.projection.union(leftAllVars);
                if (!m.filters.isEmpty()) {
                    if (vars == leftAllVars)
                        vars = Vars.fromSet(vars);
                    for (Expr e : m.filters)
                        vars.addAll(e.vars());
                }
                yield vars;
            }
            default -> {
                Vars union = new Vars.Mutable(10);
                for (int i = 0, n = opCount(); i < n; i++) union.addAll(op(i).allVars());
                yield union;
            }
        };
    }

    /** Get read-only access to the list of listeners */
    public List<MetricsListener> listeners() { return listeners; }

    /** Add {@code listener} to be notified when {@link Plan#execute(BatchType, boolean)} executions complete. */
    @SuppressWarnings("unused")
    public void attach(MetricsListener listener) {
        (listeners.isEmpty() ? listeners = new ArrayList<>() : listeners).add(listener);
    }

    /** Equivalent to {@code collection.forEach(this::attach)} */
    @SuppressWarnings("unused")
    public @This Plan attach(Collection<? extends MetricsListener> collection) {
        (listeners.isEmpty() ? listeners = new ArrayList<>() : listeners).addAll(collection);
        return this;
    }

    @Override public Plan toDistinct(DistinctType distinct) {
        if (this instanceof Modifier m) {
            if (m.distinct == distinct) return m;
            return new Modifier(m.left, m.projection, distinct, m.offset, m.limit, m.filters);
        }
        return new Modifier(this, null, distinct, 0, Long.MAX_VALUE, null);
    }

    @Override public Plan toAsk() {
        if (this instanceof Modifier m) {
            if (m.limit == 1 && m.projection == Vars.EMPTY)
                return this;
            return new Modifier(m.left, Vars.EMPTY, null, m.offset, 1, m.filters);
        } else if (this instanceof Query q) {
            if (q.sparql.isAsk()) return this;
            return new Query(q.sparql.toAsk(), q.client);
        }
        return new Modifier(this, Vars.EMPTY, null, 0, 1, null);
    }

    /**
     * Create a shallow copy of this {@link Plan}, i.e., @{code this} and the copy will point
     * to the same operand {@link Plan}s, but {@link Plan#replace(int, Plan)} in one will not be
     * seen on the other.
     */
    public abstract Plan copy(@Nullable Plan[] operands);
    public final Plan copy() { return copy(null); }

    public interface Transformer<C> {
        default Plan before(Plan parent, C context) { return parent; }
        default Plan after(Plan transformedParent, C context, boolean copied) {
            return transformedParent;
        }
    }

    public final <C> Plan transform(Transformer<C> transformer, C context) {
        Plan transformed = transformer.before(this, context);
        if (transformed != this)
            return transformed;
        for (int i = 0, n = opCount(); i < n; i++) {
            Plan o = op(i), t = o.transform(transformer, context);
            if (t == o) continue;
            if (transformed == this) transformed = copy();
            transformed.replace(i, t);
        }
        return transformer.after(transformed, context, transformed != this);
    }

    /** Performs {@code this.copy()} and the recursively {@link Plan#copy()} all operands. */
    public final Plan deepCopy() {
        Plan copy = copy();
        if (this instanceof Modifier m) {
            Modifier cm = (Modifier) copy;
            if (m.filters    instanceof ArrayList<Expr> al) cm.filters    = new ArrayList<>(al);
            if (m.projection instanceof Vars.Mutable     v) cm.projection =    Vars.fromSet(v);
        } else if (this instanceof Join j && j.projection instanceof Vars.Mutable v) {
            ((Join)copy).projection = Vars.fromSet(v);
        }
        for (int i = 0, n = copy.opCount(); i < n; i++)
            copy.replace(i, copy.op(i).deepCopy());
        return copy;
    }

    private static final Transformer<Binding> boundTransformer = new Transformer<>() {
        @Override public Plan before(Plan parent, Binding binding) {
            return switch (parent.type) {
                case QUERY -> {
                    Query q = (Query) parent;
                    SparqlQuery sq = q.sparql, bsq = sq.bound(binding);
                    yield bsq == sq ? q : new Query(bsq, q.client);
                }
                case EMPTY -> parent.boundEmpty(binding);
                case TRIPLE -> {
                    var t = (TriplePattern) parent;
                    Term s = binding.getIf(t.s), p = binding.getIf(t.p), o = binding.getIf(t.o);
                    yield s == t.s && p == t.p && o == t.o ? parent : new TriplePattern(s, p, o);
                }
                default -> parent;
            };
        }

        @Override public Plan after(Plan parent, Binding binding, boolean copied) {
            return switch (parent.type) {
                case MODIFIER -> {
                    Modifier m = (Modifier) parent;
                    Vars projection = m.projection, bVars = binding.vars();
                    if (projection != null && bVars.intersects(projection)) {
                        if (!copied) {
                            m = m.copy(null);
                            copied = true;
                        }
                        m.projection = projection.minus(bVars);
                    }
                    List<Expr> boundFilters = m.boundFilters(binding);
                    if (boundFilters != m.filters) {
                        if (!copied) m = m.copy(null);
                        m.filters = boundFilters;
                    }
                    yield m;
                }
                case JOIN -> {
                    Join j = (Join) parent;
                    Vars projection = j.projection, bVars = binding.vars();
                    if (projection != null && bVars.intersects(projection)) {
                        if (!copied) j = j.copy(null);
                        j.projection = projection.minus(bVars);
                    }
                    yield j;
                }
                default -> parent;
            };
        }
    };

    private Empty boundEmpty(Binding binding) {
        Empty e = (Empty) this;
        Vars bv = binding.vars(), bp = e.publicVars.minus(bv), ba = e.allVars.minus(bv);
        return bp == e.publicVars && ba == e.allVars ? e : new Empty(bp, ba);
    }

    @Override public final Plan bound(Binding bindings) {
        return transform(boundTransformer, bindings);
    }

    /* --- --- --- java.lang.Object methods --- --- --- */

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof Plan r) || type != r.type) return false;
        int n = opCount();
        if (n != r.opCount()) return false;
        for (int i = 0; i < n; i++) {
            if (!op(i).equals(r.op(i))) return false;
        }
        return true;
    }

    @Override public int hashCode() {
        int h = type.ordinal();
        for (int i = 0, n = opCount(); i < n; i++)
            h = 31*h + op(i).hashCode();
        return h;
    }

    @Override public String toString() {
        if (left == null)
            return algebraName().toString();
        var sb = new StringBuilder().append(algebraName()).append("(\n");
        for (int i = 0, n = opCount(); i < n; i++)
            indent(sb, op(i).toString()).append(",\n");
        sb.setLength(sb.length()-2);
        return sb.append("\n)").toString();
    }

    /* --- --- --- pure abstract methods --- --- --- */

    /** Create a {@link BIt} over the results from this plan execution. */
    public abstract <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean weakDedup);

    /** See {@link #execute(BatchType, Binding, boolean)} */
    public final <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, boolean canDedup) { return execute(bt, null, canDedup); }
    /** See {@link #execute(BatchType, Binding, boolean)} */
    public final <B extends Batch<B>> BIt<B> execute(BatchType<B> bt) { return execute(bt, false); }
    /** See {@link #execute(BatchType, Binding, boolean)} */
    @SuppressWarnings("unused") public final <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, Binding binding) { return execute(bt, binding, false); }

    public abstract <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> type, Vars rebindHint,
                                                           boolean weakDedup);

    /**
     * Create a unstarted {@link Emitter} that will produce the solutions for this plan.
     *
     * @param type The {@link BatchType} of batches to be emitted
     * @param weakDedup Allows speculative early de-duplication of rows in all execution steps.
     * @return an unstarted {@link Emitter}
     * @param <B> the batch type to be emitted
     */
    public final <B extends Batch<B>> Emitter<B> emit(BatchType<B> type, Vars rebindHint,
                                                      boolean weakDedup) {
        Emitter<B> em = doEmit(type, rebindHint, weakDedup);
        if (this.listeners.isEmpty()) return em;
        return new MetricsStage<>(em, new Metrics(this));
    }

    /** See {@link #emit(BatchType, Vars, boolean)} */
    public final <B extends Batch<B>> Emitter<B> emit(BatchType<B> type, Vars rebindHint) {
        return emit(type, rebindHint, false);
    }

    /* --- --- --- helpers --- --- --- */

    private StringBuilder indent(StringBuilder sb, String string) {
        for (int start = 0, i, len = string.length(); start < len; start = i+1) {
            i = string.indexOf('\n', start);
            if (i == -1) i = string.length();
            sb.append("  ");
            sb.append(string, start, i);
            sb.append('\n');
        }
        sb.setLength(Math.max(0, sb.length()-1));
        return sb;
    }
}
