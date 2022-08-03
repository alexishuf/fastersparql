package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOps;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.MergePlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.UnionPlan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class NativeBindHelper {
    private static <R> @Nullable Results<R> tryNativeBind(Plan<R> joinPlan, BindType bindType,
                                                          Results<R> left, Plan<R> right) {
        if (right instanceof LeafPlan) {
            LeafPlan<R> lp = (LeafPlan<R>) right;
            SparqlClient<R, ?> client = lp.client();
            if (client.usesBindingAwareProtocol())
                return client.query(lp.query(), lp.configuration(), left, bindType);
        } else if (right instanceof MergePlan || right instanceof UnionPlan) {
            NativeJoinPublisher<R> pub = NativeJoinPublisher.tryCreate(joinPlan, left);
            if (pub != null) {
                List<String> vars = bindType.resultVars(left.vars(), right.publicVars());
                return new Results<>(vars, joinPlan.rowClass(), pub);
            }
        }
        return null;
    }

    public static <R> Results<R> preferNative(RowOperations rowOps, int bindConcurrency,
                                              Plan<R> join) {
        BindType type = FasterSparqlOps.bindTypeOf(join);
        if (type == null)
            throw new IllegalArgumentException("Unsupported: join.getClass()="+join.getClass());
        Plan<R> left = join.operands().get(0), right = join.operands().get(1);
        Results<R> leftResults = left.execute();
        Results<R> results = tryNativeBind(join, type, leftResults, right);
        if (results == null) {
            BindJoinPublisher<R> pub = new BindJoinPublisher<>(rowOps, leftResults, type,
                                                               right, bindConcurrency, join.name());
            List<String> outVars = type.resultVars(leftResults.vars(), right.publicVars());
            results = new Results<>(outVars, left.rowClass(), pub);
        }
        return results;
    }
}
