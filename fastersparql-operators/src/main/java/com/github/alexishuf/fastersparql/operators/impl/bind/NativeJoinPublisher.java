package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowHashWindowSet;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.model.row.RowSet;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.TeeProcessor;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOps;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.MergePlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class NativeJoinPublisher<T> extends MergePublisher<T> {

    private final BindType bindType;
    private final Results<T> left;
    private final List<LeafPlan<T>> right;
    private boolean subscribed = false;
    private final Function<Results<T>, Publisher<T>> decorator;


    private NativeJoinPublisher(Plan<T> joinPlan, BindType bindType, Results<T> left,
                                List<LeafPlan<T>> right,
                                @Nullable Function<Results<T>, Publisher<T>> decorator) {
        super("native-"+joinPlan.name(), right.size(), right.size(),
              false, null);
        this.bindType = bindType;
        this.left = left;
        this.right = right;
        this.decorator = decorator == null ? Results::publisher : decorator;
    }

    @SuppressWarnings("unchecked")
    public static <R> @Nullable NativeJoinPublisher<R>
    tryCreate(Plan<R> joinPlan, Results<R> left, BindType bindType, Plan<R> right) {
        List<? extends Plan<R>> rightOperands = right.operands();
        for (Plan<R> r : rightOperands) {
            if (!(r instanceof LeafPlan) || !((LeafPlan<R>)r).client().usesBindingAwareProtocol())
                return null;
        }
        Function<Results<R>, Publisher<R>> decorator;
        if (right instanceof MergePlan) {
            Class<? super R> rowClass = left.rowClass();
            if (rowClass.equals(Object.class)) {
                rowClass = rightOperands.stream().map(o -> ((LeafPlan<R>)o).client().rowClass())
                        .filter(c -> !Object.class.equals(c))
                        .findFirst().orElse((Class<R>) rowClass);
            }
            RowOperations rowOps = RowOperationsRegistry.get().forClass(rowClass);
            decorator = new MergeDecorator<>(rightOperands.size(), rowOps);
        } else {
            decorator = Results::publisher;
        }

        //noinspection unchecked
        return new NativeJoinPublisher<>(joinPlan, bindType, left,
                (List<LeafPlan<R>>) rightOperands, decorator);
    }
    public static <R> @Nullable NativeJoinPublisher<R>
    tryCreate(Plan<R> joinPlan, Results<R> left) {
        BindType bindType = FasterSparqlOps.bindTypeOf(joinPlan);
        if (bindType == null)
            throw new IllegalArgumentException("Cannot implement via bind join: "+joinPlan);
        Plan<R> right = joinPlan.operands().get(1);
        return tryCreate(joinPlan, left, bindType, right);
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        if (!subscribed) {
            subscribed = true;
            TeeProcessor<T> tee = new TeeProcessor<>(left.publisher())
                    .errorOnLostItems()
                    .startAfterSubscribedBy(right.size());
            Results<T> teeRes = new Results<>(left.vars(), left.rowClass(), tee);
            for (LeafPlan<T> r : right) {
                Results<T> rRes = r.client().query(r.query(), r.configuration(), teeRes, bindType);
                addPublisher(decorator.apply(rRes));
            }
            markCompletable();
        }
        super.subscribe(s);
    }

    static final class MergeDecorator<R> implements Function<Results<R>, Publisher<R>> {
        private final List<RowSet<R>> windows;
        private final int windowSize;
        private final RowOperations rowOps;

        public MergeDecorator(int operands, RowOperations rowOps) {
            this.windowSize = FasterSparqlOpProperties.mergeWindow();
            this.rowOps = rowOps;
            this.windows = new ArrayList<>(operands);
        }

        @Override public Publisher<R> apply(Results<R> rResults) {
            return new MergeProcessor(rResults.publisher());
        }

        @Override public String toString() {
            return String.format("MergeDecorator{%d windows of %d}", windows.size(), windowSize);
        }

        private class MergeProcessor extends AbstractProcessor<R, R> {
            private final int idx;

            public MergeProcessor(FSPublisher<? extends R> src) {
                super(src);
                this.idx = windows.size();
                windows.add(new RowHashWindowSet<>(windowSize, rowOps));
            }

            @Override protected void handleOnNext(R row) {
                boolean novel = true;
                for (int i = 0, nWindows = windows.size(); novel && i < nWindows; i++)
                    novel = i == idx || !windows.get(i).contains(row);
                if (novel) {
                    windows.get(idx).add(row);
                    emit(row);
                } else {
                    upstream.request(1);
                }
            }

            @Override protected void completeDownstream(@Nullable Throwable cause) {
                super.completeDownstream(cause);
            }

            @Override public String toString() {
                return String.format("MergeProcessor[%d](%s)", idx, source);
            }
        }
    }
}
