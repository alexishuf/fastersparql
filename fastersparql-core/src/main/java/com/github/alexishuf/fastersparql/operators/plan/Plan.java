package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.util.Skip;
import com.github.alexishuf.fastersparql.operators.FSOps;
import com.github.alexishuf.fastersparql.operators.FSOpsProperties;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.parser.TriplePattern;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a tree of operators applied to their arguments
 * @param <R>
 */
public abstract class Plan<R, I> implements SparqlQuery {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    public final RowType<R, I> rowType;
    public final List<? extends Plan<R, I>> operands;
    public final @Nullable Plan<R, I> unbound;
    protected @Nullable String name; // used by subclasses and FSOps
    private @Nullable Vars publicVars, allVars;

    protected Plan(RowType<R, I> rowType, List<? extends Plan<R, I>> operands,
                   @Nullable Plan<R, I> unbound, @Nullable String name) {
        this.rowType = rowType;
        this.operands = operands;
        this.unbound = unbound;
        this.name = name;
    }

    /** A name for this plan. */
    public final String name() {
        if (name == null)
            name = getClass().getSimpleName()+"-"+nextId.getAndIncrement();
        return name;
    }

    /** Operator name to be used in {@link Plan#toString()} */
    public String algebraName() { return getClass().getSimpleName(); }

    /**
     * Get {@code parent} in the {@code parent.bind()} call which created this plan, if this
     * was created by a bind operation.
     *
     * @return the original unbound {@link Plan}, or null if this was not create by a bind.
     */
    public final @Nullable Plan<R, I> unbound() { return unbound; }

    /** Set of functions to manipulate rows produced by {@link Plan#execute(boolean)} */
    public final RowType<R, I> rowOperations() { return rowType; }

    /** {@link BIt#elementClass()} of {@link Plan#execute(boolean)}; */
    public final Class<R> rowClass() { return rowType.rowClass(); }

    @Override public String sparql() {
        StringBuilder sb = new StringBuilder(256).append("SELECT *");
        groupGraphPattern(sb, 0);
        return sb.toString();
    }

    /**
     * Write this plan as a {@code GroupGraphPattern} production from the SPARQL grammar
     *
     * @param out where to write the SPARQL to.
     */
    public final void groupGraphPattern(StringBuilder out, int indent) {
        newline(out, indent++).append('{');
        groupGraphPatternInner(out, indent);
        newline(out, --indent).append('}');
    }

    /**
     * Equivalent to {@code groupGraphPattern(out, indent)} without the surrounding
     * {@code '{'} and {@code'}'}.
     */
    public void groupGraphPatternInner(StringBuilder out, int indent) {
        if (isBGPSuffix()) {
            ArrayDeque<Plan<R, I>> stack = new ArrayDeque<>();
            var o = this;
            for (; o.isBGPSuffix(); o = o.operands.get(0))
                stack.push(o);
            if (o instanceof Join<R, I> j)
                j.groupGraphPatternInner(out, indent);
            else if (o instanceof TriplePattern<R,I> tp)
                tp.groupGraphPatternInner(out, indent);
            else
                o.groupGraphPattern(out, indent);
            while (!stack.isEmpty()) {
                Plan<R, I> m = stack.pop();
                m.bgpSuffix(out, indent);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /** This algebra does not support graph queries */
    @Override public boolean isGraph() { return false; }

    /**
     * The would-be value of {@link BIt#vars()} upon {@link Plan#execute(boolean)}.
     *
     * @return a non-null (but possibly empty) list of non-null and non-empty variable names
     *         (i.e., no leading {@code ?} or {@code $}).
     */
    @Override public final Vars publicVars() {
        if (publicVars == null) publicVars = computeVars(false);
        return publicVars;
    }

    /**
     * All vars used within this plan, not only those exposed in results.
     *
     * <p>This is the list of variables that should be used with {@link Plan#bind(Binding)} and related
     * methods.</p>
     *
     * @return a non-null (possibly empty) list of non-null and non-empty variable names
     *         (i.e., no preceding {@code ?} or {@code $}).
     */
    @Override public final Vars allVars() {
        if (allVars == null) allVars = computeVars(true);
        return allVars;
    }

    /**
     * Child operands of this plan.
     *
     * @return a non-null immutable and possibly empty list of non-null plans.
     */
    public final List<? extends Plan<R, I>> operands() { return operands; }

    /** Get a copy of this plan replacing the operands. */
    public final Plan<R, I> with(List<? extends Plan<R, I>> replacement) {
        return replacement == operands ? this : with(replacement, null, null);
    }
    /** Get a copy of this plan replacing the operands and {@link Plan#unbound()} */
    public final Plan<R, I> with(List<? extends Plan<R, I>> replacement, Plan<R, I> unbound) {
        if (replacement == operands && (unbound == null || unbound == this.unbound))
            return this;
        return with(replacement, unbound, null);
    }
    /** Get a copy of this plan replacing {@link Plan#unbound()} */
    public final Plan<R, I> with(Plan<R, I> unbound) {
        boolean noOp = unbound == null || unbound == this.unbound;
        return noOp ? this : with(operands, unbound, null);
    }
    /** Get a copy of this plan replacing {@link Plan#name()} */
    public final Plan<R, I> with(String name) {
        //noinspection StringEquality
        boolean noOp = name == null || name == this.name;
        return noOp ? this : with(operands, unbound, name);
    }

    @Override public Plan<R, I> toDistinct(DistinctType distinctType) {
        if (this instanceof Modifier<R,I> m) {
            int capacity = switch (distinctType) {
                case WEAK -> FSOpsProperties.reducedCapacity();
                case STRONG -> Integer.MAX_VALUE;
            };
            if (m.distinctCapacity == capacity)
                return this;
        }
        return switch (distinctType) {
            case WEAK -> FSOps.reduced(this);
            case STRONG -> FSOps.distinct(this);
        };
    }

    @Override public Plan<R, I> toAsk() {
        if (this instanceof Modifier<R,I> m && m.limit == 1 && m.projection == Vars.EMPTY)
            return this;
        return FSOps.modifiers(this, Vars.EMPTY, 0, 0, 1, List.of());
    }

    /**
     * Create a copy of this {@link Plan} replacing the variables with the values they map to.
     *
     * @param binding a mapping from variable names to RDF terms in N-Triples syntax.
     * @return a non-null Plan, being a copy of this with replaced variables or {@code this}
     *         if there is no variable to replace.
     */
    @Override public Plan<R, I> bind(Binding binding) {
        int size = operands.size();
        if (size == 1) {
            Plan<R, I> input = operands.get(0);
            Plan<R, I> bound = input.bind(binding);
            return bound == input ? this : with(List.of(bound), this);
        } else {
            List<Plan<R, I>> boundList = new ArrayList<>(size);
            boolean change = false;
            for (Plan<R, I> plan : operands) {
                Plan<R, I> bound = plan.bind(binding);
                change |= bound != plan;
                boundList.add(bound);
            }
            return change ? with(boundList, this) : this;
        }
    }

    /* --- --- --- java.lang.Object methods --- --- --- */

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Plan<?, ?> p) || !rowType.equals(p.rowType)) return false;
        if (name != null && p.name != null && !name.equals(p.name)) return false;
        return Objects.equals(unbound, p.unbound) && operands.equals(p.operands);
    }

    @Override public int hashCode() { return Objects.hash(rowType, unbound, operands); }

    @Override public String toString() {
        if (operands.isEmpty())
            return algebraName();
        StringBuilder sb = new StringBuilder().append(algebraName()).append("(\n");
        for (Plan<R, I> op : operands)
            indent(sb, op.toString()).append(",\n");
        sb.setLength(sb.length()-2);
        return sb.append("\n)").toString();
    }

    /* --- --- --- pure abstract methods --- --- --- */

    /** Create a {@link BIt} over the results from this plan execution. */
    public abstract BIt<R> execute(boolean canDedup);

    public final BIt<R> execute() { return execute(false); }

    /** Create a copy of this plan replacing the operands, {@link Plan#unbound()}
     *  and {@link Plan#name()} */
    public abstract Plan<R, I> with(List<? extends Plan<R, I>> replacement,
                                 @Nullable Plan<R, I> unbound, @Nullable String name);

    /* --- --- --- overridable protected methods --- --- --- */

    /** Compute {@link Plan#allVars()} (if {@code all==true}), else {@link Plan#publicVars()} */
    protected Vars computeVars(boolean all) {
        int n = operands.size();
        return switch (n) {
            case 0 -> Vars.EMPTY;
            case 1 -> all ? operands.get(0).allVars() : operands.get(0).publicVars();
            default -> {
                var first = all ? operands.get(0).allVars() : operands.get(0).publicVars();
                Vars.Mutable union = Vars.fromSet(first, Math.max(10, first.size() + 4));
                for (int i = 1; i < n; i++)
                    union.addAll(all ? operands.get(i).allVars() : operands.get(i).publicVars());
                yield union;
            }
        };
    }

    /* --- --- --- helpers --- --- --- */

    private static final String[] LINE_BREAKS = {
            "\n", "\n ", "\n  ", "\n   ", "\n    ", "\n     ", "\n      ", "\n       ",
            "\n        ", "\n         ", "\n          ", "\n           ", "\n            ",
            "\n             ", "\n              ", "               ", "                ",
    };

    /** Writes {@code '\n'} followed by {@code indent} {@code ' '}s to {@code out}*/
    protected final StringBuilder newline(StringBuilder b, int indent) {
        if (indent < LINE_BREAKS.length)
            return b.append(LINE_BREAKS[indent]);
        else
            return b.append('\n').append(" ".repeat(indent));
    }

    /** If this is a {@link Join} or {@link Union}, return a list of operands replacing children
     *  of the same type with their children */
    protected final List<? extends Plan<R, I>> flatOperands() {
        List<Plan<R, I>> flat = null;
        Class<?> cls;
        if (this instanceof Join<R,I>)
            cls = Join.class;
        else if (this instanceof Union<R,I>)
            cls = Union.class;
        else
            return operands;
        for (int i = 0, n = operands.size(); i < n; i++) {
            Plan<R, I> o = operands.get(i);
            if (cls.isInstance(o)) {
                if (flat == null) {
                    flat = new ArrayList<>();
                    for (int j = 0; j < i; j++)
                        flat.add(operands.get(j));
                }
                flat.addAll(o.flatOperands());
            } else if (flat != null) {
                flat.add(o);
            }
        }
        return flat == null ? operands : flat;
    }

    /** Whether this operator forgoes nesting in SPARQL syntax. */
    private boolean isBGPSuffix() {
        return this instanceof Exists || this instanceof LeftJoin ||
               this instanceof Minus  || this instanceof Modifier;
    }

    protected void bgpSuffix(StringBuilder out, int indent) {
        throw new UnsupportedOperationException();
    }

    private StringBuilder indent(StringBuilder sb, String string) {
        for (int start = 0, i, len = string.length(); start < len; start = i+1) {
            i = Skip.skipUntil(string, start, len, '\n');
            sb.append("  ");
            sb.append(string, start, i);
            sb.append('\n');
        }
        sb.setLength(Math.max(0, sb.length()-1));
        return sb;
    }
}
