package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

public abstract class CardinalityEstimator {
    public static final Term GROUND = Term.iri("http://example.org/ground");
    public static final String TYPE = "type";

    /* --- --- --- load from spec --- --- --- */

    public static CardinalityEstimator load(SparqlClient client, Spec spec) throws IOException {
        String name = spec.getOr(TYPE, PatternCardinalityEstimator.NAME);
        return NSL.get(name).load(client, spec);
    }

    interface Loader extends NamedService<String> {
        CardinalityEstimator load(SparqlClient client, Spec spec) throws IOException, BadSerializationException;
    }

    private static final NamedServiceLoader<Loader, String> NSL = new NamedServiceLoader<>(Loader.class) {
        @Override protected Loader fallback(String name) {
            throw new BadSerializationException.UnknownEstimator(name);
        }
    };

    /* --- --- --- fields --- --- --- */
    private final int lowDistinctCap = 2*FSProperties.dedupCapacity();
    private final int highDistinctCap = 2*FSProperties.reducedCapacity();

    /* --- --- --- cardinality estimation --- --- --- */

    final int estimate(Plan plan)        { return estimate(plan, null); }
    final int estimate(TriplePattern tp) { return estimate(tp, null); }


    public abstract int estimate(TriplePattern tp, @Nullable Binding binding);
    public int estimate(Query q, @Nullable Binding binding) {
        SparqlQuery sparql = q.sparql;
        if (sparql instanceof Plan p) return estimate(p, binding);
        return estimate(new SparqlParser().parse(sparql), binding);
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
        int accCost = estimate(plan.op(0), accBinding);
        for (int i = 1, n = plan.opCount(); i < n; i++) {
            Plan o = plan.op(i);
            int half = 1 + estimate(o, accBinding) >> shift;
            accCost = (int)Math.min(Integer.MAX_VALUE, accCost*(long)half);
            for (Rope name : o.publicVars()) {
                int varIdx = accBinding.vars.indexOf(name);
                if (accBinding.get(varIdx) == null)
                    accBinding.set(varIdx, GROUND);
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
            case MINUS,EXISTS,NOT_EXISTS -> estimateJoin(plan, binding, 4);
            case JOIN,LEFT_JOIN -> estimateJoin(plan, binding, 1);
        };
    }

}
