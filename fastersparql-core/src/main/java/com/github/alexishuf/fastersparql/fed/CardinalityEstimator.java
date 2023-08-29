package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.model.Vars;
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

import static com.github.alexishuf.fastersparql.sparql.expr.Term.GROUND;


public abstract class CardinalityEstimator {
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
    private final int lowDistinctCap = 2*FSProperties.dedupCapacity();
    private final int highDistinctCap = 2*FSProperties.reducedCapacity();
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
            int len = (int) Math.max(Integer.MAX_VALUE, modifier.limit + modifier.offset);
            if (len == 1) // ASK costs 1/16 (6.25%) of original estimate
                return 1 + cost>>4;
            if (len < cost) // drop down cost to avg(offset+limit, cost)/2
                cost = (cost+len)>>2;
        }

        // If distinct capacity is too low or too high, reduce cost by 1/8 (12.5%)
        // else if capacity within a range that will drop results without reasonable
        // memory overhead, reduce cost by 1/4 (25%).
        int dCap = modifier.distinctCapacity;
        if (dCap > 0)
            cost -= cost>>(dCap > lowDistinctCap && dCap < highDistinctCap ? 2 : 4);
        return cost;
    }

    protected int estimateJoin(Plan plan, @Nullable Binding binding, int shift) {
        var accBinding = new ArrayBinding(plan.allVars(), binding);
        int accCost = estimate(plan.left(), accBinding);
        for (var name : plan.left().publicVars()) {
            int varIdx = accBinding.vars.indexOf(name);
            if (accBinding.get(varIdx) == null)
                accBinding.set(varIdx, GROUND);
        }
        for (int i = 1, n = plan.opCount(); i < n; i++) {
            Plan o = plan.op(i);
            int cost = estimate(o, accBinding);
            Vars oVars = o.publicVars();
            boolean noNewVars = true, cartesian = true;
            short unjoinedNewVars = 0, joins = 0;
            for (var name : oVars) {
                int varIdx = accBinding.vars.indexOf(name);
                if (accBinding.get(varIdx) == null) {
                    accBinding.set(varIdx, GROUND);
                    noNewVars = false;
                    boolean unjoined = true;
                    for (int j = i+1; j < n; j++) {
                        Plan oo = plan.op(j);
                        Vars ooVars = (oo instanceof Modifier ? oo.left() : oo).allVars();
                        if (ooVars.contains(name)) {
                            ++joins;
                            unjoined = false;
                        }
                    }
                    if (unjoined) ++unjoinedNewVars;
                } else {
                    cartesian = false;
                }
            }
            if (noNewVars) {
                if ((o instanceof Modifier m ? m.left() : o).allVars().equals(oVars))
                    accCost -= accCost >> 4; // o is acting as a filter
                else
                    accCost = accCost + cost; // o has a small multiplicative effect, thus we add
            } else if (cartesian) {
                // cartesian product may only have a filtering effect in theory
                accCost *= cost;
            } else if (joins == 0) {
                // new vars were added, but they do not contribute to any subsequent join.
                // assume these vars will not cause filtering. For > 1 new vars, assume that
                // half of them have multiplicative effect and will introduce an additional row
                accCost += cost + (accCost*(unjoinedNewVars >> 1));
            } else {
                // always assume that a join that introduces vars that feed another
                // operand is multiplicative. In order to not be overly pessimistic,
                // assume that for some bindings, o will produce no match. This is emulated by
                // the shift. If a join involves more than one var, it may produce less results
                // as each var begins a path and all paths must exist.
                accCost *= Math.max(2, (cost >> shift) - (joins-1));
            }
        }
        return accCost;
    }

    public int estimate(Plan plan, @Nullable Binding binding) {
        return switch (plan.type) {
            case EMPTY -> 0;
            case VALUES -> ((Values)plan).values().rows;
            case TRIPLE -> estimate((TriplePattern) plan, binding);
            case QUERY -> estimate((Query)plan, binding);
            case MODIFIER -> estimateModifier((Modifier)plan, binding);
            case UNION -> {
                int sum = 0;
                for (int i = 0, n = plan.opCount(); i < n; i++)
                    sum += estimate(plan.op(i), binding);
                yield sum;
            }
            case MINUS,EXISTS,NOT_EXISTS -> estimateJoin(plan, binding, 8);
            case JOIN,LEFT_JOIN -> estimateJoin(plan, binding, 2);
        };
    }

}
