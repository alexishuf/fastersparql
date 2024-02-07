package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static java.lang.Math.max;
import static java.lang.Math.min;


public abstract class CardinalityEstimator {
    protected static final long I_MAX = Integer.MAX_VALUE;
    public static final String TYPE = "type";
    public static final String PREFER_NATIVE = "prefer-native";

    /* --- --- --- load from spec --- --- --- */

    public static CardinalityEstimator load(SparqlClient client, Spec spec) throws IOException {
        if (client instanceof CardinalityEstimatorProvider p && spec.getBool(PREFER_NATIVE))
            return p.estimator();
        String name = spec.getOr(TYPE, PatternCardinalityEstimator.NAME);
        return NSL.get(name).load(client, spec);
    }

    public interface Loader extends NamedService<String> {
        CardinalityEstimator load(SparqlClient client, Spec spec) throws BadSerializationException;
    }

    private static final NamedServiceLoader<Loader, String> NSL = new NamedServiceLoader<>(Loader.class) {
        @Override protected Loader fallback(String name) {
            throw new BadSerializationException.UnknownEstimator(name);
        }
    };

    /* --- --- --- fields & lifecycle --- --- --- */
    protected final CompletableFuture<CardinalityEstimator> ready;

    public CardinalityEstimator(CompletableFuture<CardinalityEstimator> ready) {
        this.ready = ready;
    }

    public CompletionStage<? extends CardinalityEstimator> ready() { return ready; }

    /* --- --- --- cardinality estimation --- --- --- */

    final int estimate(Plan plan)        { return estimate(plan, null); }
    final int estimate(TriplePattern tp) { return estimate(tp, null); }


    public abstract int estimate(TriplePattern tp, @Nullable Binding binding);
    public int estimate(Query q, @Nullable Binding binding) {
        var parsed = q.sparql instanceof Plan p ? p : SparqlParser.parse(q.sparql);
        return estimate(parsed, binding);
    }

    protected int estimateModifier(Modifier modifier, @Nullable Binding binding) {
        int cost = estimate(modifier.left(), binding);
        // for each filter, reduce cost by 1/16 (6.25%), but the discount cannot exceed 25%.
        // if we meet a regex increase the cost by 12.5%.
        int filterBonus = cost>>4, accFilterBonus = 0;
        for (Expr e : modifier.filters)
            accFilterBonus += e instanceof Expr.Regex ? -(filterBonus<<1) : filterBonus;
        cost -= Math.min(accFilterBonus, cost>>2);

        // discounts for ASK and LIMIT (with or without OFFSET)
        if (modifier.limit < Long.MAX_VALUE) {
            int len = (int) max(Integer.MAX_VALUE, modifier.limit + modifier.offset);
            if (len == 1) // ASK costs 1/16 (6.25%) of original estimate
                return 1 + cost>>4;
            if (len < cost) // drop down cost to avg(offset+limit, cost)/2
                cost = (cost+len)>>2;
        }

        // Apply a bonus depending on selectivity and cost of de-duplication
        cost -= switch (modifier.distinct) {
            case WEAK -> cost>>3; // 12.50% bonus (1/8) WeakDedup is fast but not that selective
            case REDUCED -> cost>>2; // 25.00% bonus (1/4) still fast, but more selective
            case STRONG -> cost>>4; //  6.25% bonus (1/16) selective, but dreadfully slow slow
            case null   -> 0;
        };
        return cost;
    }

    /**
     * Compute the cost of joining with a right side that has no unbound
     * public or private vars. Such join behaves as an EXISTS filter, and evalution of the
     * right side will be cheap.
     */
    protected static int joinWithAsk(long leftCost, int rightAllVars) {
        int div = rightAllVars <= 2 ? 2 : rightAllVars <= 4 ? 3 : 4;
        return (int)min(I_MAX, leftCost + rightAllVars - (leftCost >> div));
    }

    protected static Plan unwrapQuery(Plan plan) {
        return plan instanceof Query q && q.sparql instanceof Plan p ? p : plan;
    }

    private static Plan unwrapQueryOrMod(Plan plan) {
        plan = unwrapQuery(plan);
        return plan.type == Operator.MODIFIER ? plan.left() : plan;
    }

    private static boolean hasStarJoin(TriplePattern t, Plan plan) {
        if (plan instanceof TriplePattern t1)
            return t.s.equals(t1.s) || t.o.equals(t1.o);
        for (int i = 0, n = plan.opCount(); i < n; i++) {
            if ((plan.op(i) instanceof TriplePattern t1) && (t.s.equals(t1.s) || t.o.equals(t1.o)))
                return true;
        }
        return false;
    }
    private static boolean hasStarJoin(Plan left, Plan right) {
        if (left instanceof TriplePattern lTP)
            return hasStarJoin(lTP, right);
        else if (right instanceof TriplePattern rTP)
            return hasStarJoin(rTP, left);
        for (int i = 0, n = left.opCount(); i < n; i++) {
            if (left.op(i) instanceof TriplePattern t && hasStarJoin(t, right)) return true;
        }
        return false;
    }

    private static boolean hasPathJoin(TriplePattern t0, Plan plan) {
        if (plan instanceof TriplePattern t)
            return t0.s.equals(t.o) || t0.o.equals(t.s);
        for (int i = 0, n = plan.opCount(); i < n; i++) {
            if (plan.op(i) instanceof TriplePattern t && (t.s.equals(t0.o) || t.o.equals(t0.s)))
                return true;
        }
        return false;
    }
    private static boolean hasPathJoin(Plan left, Plan right) {
        if (left instanceof TriplePattern l)
            return hasPathJoin(l, right);
        else if (right instanceof TriplePattern r)
            return hasPathJoin(r, left);
        for (int i = 0, n = left.opCount(); i < n; i++) {
            if (left.op(i) instanceof TriplePattern t && hasPathJoin(t, right)) return true;
        }
        return false;
    }

    protected static int join(Plan prev, int prevCost, Plan right, int rightCost) {
        long total;
        Plan uwPrev = unwrapQueryOrMod(prev), uwRight = unwrapQueryOrMod(right);
        if (hasStarJoin(uwPrev, uwRight))
            return max(1, max(prevCost, rightCost)-1);
        else if (hasPathJoin(uwPrev, uwRight))
            total = (long)prevCost+rightCost;
        else
            total = (long)prevCost*rightCost;
        return (int)min(I_MAX, total);
    }
    protected static long join(Plan[] ops, int rightIndex, int accCost, int rightCost) {
        long total = (long)accCost * rightCost;
        Plan right = unwrapQueryOrMod(ops[rightIndex]);
        boolean star = false, path = false;
        for (int i = 0; !star && i < rightIndex; i++) {
            Plan prev = unwrapQueryOrMod(ops[i]);
            star = hasStarJoin(prev, right);
            if (!star && !path)
                path = hasPathJoin(prev, right);
        }
        if (star)
            total = max(1, max(accCost, rightCost) - 1);
        if (path)
            total = accCost+rightCost;
        return total;
    }

    /**
     * Count the number of vars in {@code right} that are not found in {@code leftPub}
     * nor have a value set in {@code binding}
     */
    protected boolean isAsk(@Nullable Binding binding, Vars leftPub, Vars right) {
        for (SegmentRope v : right) {
            if (!leftPub.contains(v) && (binding == null || !binding.has(v)))
                return false;
        }
        return true;
    }

    private static final int MAX_EXISTS_RIGHT_COST = COMPRESSED.preferredTermsPerBatch()/4;

    protected int estimateExists(Plan plan, @Nullable Binding binding) {
        Plan left = plan.left(), right = plan.right();
        Vars lPub = left.publicVars(), rAll = right.allVars();
        long lCost = estimate(left, binding);
        int rAllSize = rAll.size();
        if (isAsk(binding, lPub, rAll)) { // right side is an ASK
            return joinWithAsk(lCost, rAllSize);
        } else {
            // right side may yield more than one row. evaluating it might cost,
            // even if only 1 row is fetched.
            var accBinding = new ArrayBinding(rAll, binding);
            accBinding.ground(lPub);
            int rCost = min(MAX_EXISTS_RIGHT_COST, estimate(right, accBinding)>>7);
            return (int)min(I_MAX, lCost + rAllSize + lCost*rCost);
        }
    }

    protected final int estimateLeftJoin(Plan plan, @Nullable Binding binding) {
        Plan left = plan.left(), right = plan.right();
        int lc = estimate(left, binding);
        if (isAsk(binding, left.publicVars(), right.allVars()))
            return lc; // LeftJoin(L, R) == L
        var accBinding = new ArrayBinding(right.allVars(), binding);
        accBinding.ground(left.publicVars());
        return join(left, lc, right, estimate(right, accBinding));
    }

    protected int estimateJoin(Plan plan, @Nullable Binding binding) {
        Plan[] ops = plan.operandsArray;
        if (ops != null && ops.length > 2)
            return estimateNaryJoin(plan, ops, binding);
        Plan l = plan.left(), r = plan.right();
        var accBinding = new ArrayBinding(r.allVars(), binding);
        accBinding.ground(l.publicVars());
        return join(l, estimate(l, binding), r, estimate(r, accBinding));
    }

    protected int estimateNaryJoin(Plan plan, Plan[] ops, @Nullable Binding outerBinding) {
        var binding = new ArrayBinding(plan.allVars(), outerBinding);
        int acc = estimate(ops[0], binding);
        for (int i = 1; i < ops.length; i++) {
            binding.ground(ops[i-1].publicVars());
            Plan op = ops[i];
            Vars allVars = op.allVars();
            acc = isAsk(binding, Vars.EMPTY, allVars)
                    ? joinWithAsk(acc, allVars.size())
                    : (int)min(I_MAX, join(ops, i, acc, estimate(op, binding)));
        }
        return acc;
    }

    public int estimate(Plan plan, @Nullable Binding binding) {
        return switch (plan.type) {
            case EMPTY    -> 0;
            case VALUES   -> ((Values)plan).values().totalRows();
            case TRIPLE   -> estimate((TriplePattern) plan, binding);
            case QUERY    -> estimate((Query)plan, binding);
            case MODIFIER -> estimateModifier((Modifier)plan, binding);
            case UNION    -> {
                int sum = 0;
                for (int i = 0, n = plan.opCount(); i < n; i++)
                    sum += estimate(plan.op(i), binding);
                yield sum;
            }
            case MINUS,EXISTS,NOT_EXISTS -> estimateExists  (plan, binding);
            case LEFT_JOIN               -> estimateLeftJoin(plan, binding);
            case JOIN                    -> estimateJoin    (plan, binding);
        };
    }
//
//    /** Whether estimation for {@code ?s a :Class} are reliable */
//    public boolean isSTypeClassReliable() { return false; }
//
//    /** Whether estimation for {@code ?s :predicate ?o} are reliable */
//    public boolean isSPredicateOReliable() { return false; }

}
