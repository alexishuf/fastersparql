package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Union;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.UnionProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

@Value
public class SimpleUnion  implements Union {
    RowOperations rowOps;
    boolean parallelSubscribe;

    public static class Provider implements UnionProvider {
        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }

        @Override public Union create(long flags, RowOperations rowOperations) {
            boolean parallelSubscribe = (flags & OperatorFlags.ASYNC) != 0;
            return new SimpleUnion(rowOperations, parallelSubscribe);
        }
    }

    @Override public <R> Results<R> checkedRun(List<Plan<R>> plans) {
        List<Results<R>> results = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) results.add(plan.execute());
        List<String> unionVars = VarUtils.union(results);
        return new Results<>(unionVars, rowClass(results),
                             new UnionPublisher<>(rowOps, unionVars, results, parallelSubscribe));
    }

    private static <R> Class<? super R> rowClass(List<Results<R>> results) {
        assert results.stream().map(Results::rowClass).distinct().count() < 2
                : "More than one row class among results";
        for (Results<R> r : results)
            return r.rowClass();
        return Object.class;
    }

    private static final class Projector {
        private final RowOperations rowOps;
        private final List<String> outVars;
        private final List<String> upstreamVars;
        private final int[] indices;

        public Projector(RowOperations rowOps, List<String> outVars, List<String> upstreamVars) {
            this.rowOps = rowOps;
            this.upstreamVars = upstreamVars;
            this.outVars = outVars;
            this.indices = VarUtils.projectionIndices(outVars, upstreamVars);
        }

        public Object project(Object upstreamRow) {
            if (indices.length == 0 && !outVars.isEmpty())
                return upstreamRow;
            Object projection = rowOps.createEmpty(outVars);
            for (int i = 0; i < indices.length; i++) {
                int upstreamIdx = indices[i];
                if (upstreamIdx >= 0) {
                    Object v = rowOps.get(upstreamRow, upstreamIdx, upstreamVars.get(upstreamIdx));
                    rowOps.set(projection, i, outVars.get(i), v);
                }
            }
            return projection;
        }
    }

    @Slf4j
    private static class UnionPublisher<R> implements Publisher<R> {
        private final RowOperations rowOps;
        private final List<String> outVars;
        private final List<Results<R>> sources;
        private final boolean parallel;
        private boolean terminated, distributing;
        private int terminatedUpstreams;
        private final List<UpstreamSubscriber> upstream;
        private @MonotonicNonNull Subscriber<? super R> downstream;

        public UnionPublisher(RowOperations rowOps, List<String> unionVars,
                              List<Results<R>> sources, boolean parallel) {
            this.rowOps = rowOps;
            this.outVars = unionVars;
            this.sources = sources;
            this.parallel = parallel;
            this.upstream = new ArrayList<>(sources.size());
        }

        /* --- --- --- publisher methods --- --- --- */

        @Override public void subscribe(Subscriber<? super R> s) {
            if (downstream != null) {
                s.onSubscribe(new Subscription() {
                    @Override public void request(long n) { }
                    @Override public void cancel() { }
                });
                s.onError(new IllegalStateException(this+"does not support multiple subscribers"));
            } else {
                downstream = s;
                for (Results<R> results : sources) {
                    Projector task = new Projector(rowOps, outVars, results.vars());
                    results.publisher().subscribe(new UpstreamSubscriber(task));
                }
            }
        }

        private class  UpstreamSubscriber implements Subscriber<R> {
            private final Projector projector;
            private long requested = 0;
            private boolean active = false;
            private Subscription subscription;

            private UpstreamSubscriber(Projector projectorTask) {
                this.projector = projectorTask;
            }

            /* --- --- --- control methods --- --- --- */

            /**
             * Request {@code n} from upstream from the current thread.
             *
             * @param n how many items to request
             * @return zero if the request has been fully honored or if the
             *         subscription is still active (i.e., upstream may still produce the items).
             *         Else the return is a non-negative number of items that must be requested
             *         from another {@link UpstreamSubscriber}, since this one is not active
             *         anymore.
             */
            public synchronized long syncRequest(@Positive long n) {
                if (active) {
                    requested += n;
                    try {
                        subscription.request(n);
                    } catch (Throwable t) {
                        terminate(t);
                    }
                    return active ? 0 : requested;
                }
                return n;
            }

            /**
             * Asynchronously request {@code n} items from upstream.
             *
             * If when the request happens the upstream has already terminated, or if the request
             * occurs before termination but not all requested elements could be produced,
             * {@link UnionPublisher#distribute(long)} will be called with the number of items
             * that still need to be produced.
             *
             * @param n the number of items to request from upstream.
             */
            public void asyncRequest(@Positive long n) {
                Async.async(() -> {
                    synchronized (this) {
                        if (active) {
                            requested += n;
                            synchronized (this) {
                                try {
                                    subscription.request(n);
                                } catch (Throwable t) {
                                    terminate(t);
                                }
                            }
                        } else {
                            distribute(n);
                        }
                    }
                });
            }

            public synchronized void cancel() {
                if (active) {
                    active = false;
                    try {
                        subscription.cancel();
                    } catch (Throwable t) {
                        log.error("UpstreamSubscriber.cancel(): ignoring {} from {}.cancel()",
                                  t, subscription, t);
                    }
                }
            }

            /* --- --- --- subscriber methods --- --- --- */

            @Override public void onSubscribe(Subscription s) {
                subscription = s;
                active = true;
                addUpstream(this);
            }

            @Override public synchronized void onNext(R upstreamRow) {
                if (requested > 0)
                    --requested;
                //noinspection unchecked
                downstream.onNext((R) projector.project(upstreamRow));
            }

            @Override public synchronized void onError(Throwable t) {
                active = false;
                cancelAllUpstream();
                terminate(t);
            }

            @Override public void onComplete() {
                long distributeCount = 0;
                synchronized (this) {
                    if (active) {
                        active = false;
                        terminatedUpstream();
                        distributeCount = requested;
                    }
                }
                distribute(distributeCount);
            }
        }

        /* --- --- --- Subscription class --- --- --- */

        private class DownstreamSubscription implements Subscription {
            @Override public void request(long n) { distribute(n); }
            @Override public void cancel() { cancelAllUpstream(); }
        }

        /* --- --- --- helper methods --- --- --- */

        private synchronized void addUpstream(UpstreamSubscriber u) {
            upstream.add(u);
            if (upstream.size() == sources.size())
                downstream.onSubscribe(new DownstreamSubscription());
        }

        private synchronized void terminatedUpstream() {
            ++terminatedUpstreams;
            if (terminatedUpstreams == sources.size())
                terminate(null);
        }

        private void distribute(long n) {
            if (terminated)
                return;
            if (parallel) {
                long chunk = Math.max(1, n / upstream.size());
                for (UpstreamSubscriber s : upstream)
                    s.asyncRequest(chunk);
            } else {
                synchronized (this) {
                    // prevent stack overflow by ensuring just one distribute() call per "this"
                    // if a UpstreamSubscriber recursively needs to distribute(m), it
                    // returns m from syncRequest(n)
                    if (!distributing) {
                        distributing = true;
                        try {
                            while (!terminated && n > 0) {
                                for (UpstreamSubscriber s : upstream) {
                                    if (terminated || (n = s.syncRequest(n)) == 0)
                                        break;
                                }
                            }
                        } finally {
                            distributing = false;
                        }
                    }
                }
            }
        }

        private synchronized void cancelAllUpstream() {
            for (UpstreamSubscriber s : upstream) s.cancel();
        }

        private synchronized void terminate(@Nullable Throwable cause) {
            if (terminated)
                return;
            terminated = true;
            if (cause == null) {
                try {
                    downstream.onComplete();
                } catch (Throwable t) {
                    log.error("Ignoring {} from {}.onComplete()", t, downstream, t);
                }
            } else {
                try {
                    downstream.onError(cause);
                } catch (Throwable t) {
                    log.error("Ignoring {} from {}.onError({})", t, downstream, cause, t);
                }
            }
            cancelAllUpstream();
        }
    }
}
