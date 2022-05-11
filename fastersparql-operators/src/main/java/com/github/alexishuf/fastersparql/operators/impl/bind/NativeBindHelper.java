package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class NativeBindHelper {
    public static <R> @Nullable Results<R> tryNativeBind(BindType bindType, Results<R> left,
                                                         Plan<R> right) {
        if (right instanceof LeafPlan) {
            LeafPlan<R> lp = (LeafPlan<R>) right;
            SparqlClient<R, ?> client = lp.client();
            if (client.usesBindingAwareProtocol())
                return client.query(lp.query(), lp.configuration(), left, bindType);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <R> Results<R> preferNative(RowOperations rowOps, int bindConcurrency,
                                              BindType bindType, List<? extends Plan<R>> operands) {
        switch (operands.size()) {
            case 0: return Results.empty((Class<? super R>) rowOps.rowClass());
            case 1: return operands.get(0).execute();
            case 2: break;
            default: throw new IllegalArgumentException("expected <= 2 operands");
        }
        return preferNative(rowOps, bindConcurrency, bindType, operands.get(0), operands.get(1));
    }

    public static <R> Results<R> preferNative(RowOperations rowOps, int bindConcurrency,
                                              BindType bindType, Plan<R> left, Plan<R> right) {
        Results<R> leftResults = left.execute();
        Results<R> results = tryNativeBind(bindType, leftResults, right);
        if (results == null) {
            BindJoinPublisher<R> pub = new BindJoinPublisher<>(rowOps, leftResults, bindType,
                                                               right, bindConcurrency);
            List<String> outVars = bindType.resultVars(leftResults.vars(), right.publicVars());
            results = new Results<>(outVars, left.rowClass(), pub);
        }
        return results;
    }
}
