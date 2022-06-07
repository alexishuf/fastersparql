package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.plan.*;
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

    public static <R> Results<R> preferNative(RowOperations rowOps, int bindConcurrency,
                                              Plan<R> join) {
        BindType type;
        Plan<R> left = join.operands().get(0), right = join.operands().get(1);
        if (join instanceof LeftJoinPlan)
            type = BindType.LEFT_JOIN;
        else if (join instanceof JoinPlan)
            type = BindType.JOIN;
        else if (join instanceof MinusPlan)
            type = BindType.MINUS;
        else if (join instanceof ExistsPlan)
            type = ((ExistsPlan<R>) join).negate() ? BindType.NOT_EXISTS : BindType.EXISTS;
        else
            throw new IllegalArgumentException("Unsupported: join.getClass()="+join.getClass());
        Results<R> leftResults = left.execute();
        Results<R> results = tryNativeBind(type, leftResults, right);
        if (results == null) {
            BindJoinPublisher<R> pub = new BindJoinPublisher<>(rowOps, leftResults, type,
                                                               right, bindConcurrency, join.name());
            List<String> outVars = type.resultVars(leftResults.vars(), right.publicVars());
            results = new Results<>(outVars, left.rowClass(), pub);
        }
        return results;
    }
}
