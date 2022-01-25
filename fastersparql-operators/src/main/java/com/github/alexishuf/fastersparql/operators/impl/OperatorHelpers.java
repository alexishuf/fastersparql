package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.client.util.async.Async.wrap;
import static java.util.Collections.emptyList;

public class OperatorHelpers {
    public static <R> Results<R> errorResults(@Nullable Results<R> in,
                                              @Nullable SafeAsyncTask<List<String>> varsTask,
                                              @Nullable Class<? super R> rowClass,
                                              Throwable cause) {
        if (varsTask == null)
            varsTask = in != null ? in.vars() : wrap(emptyList());
        Class<? super R> cls = rowClass != null ? rowClass
                             : in == null ? Object.class : in.rowClass();
        return new Results<>(varsTask, cls, new EmptyPublisher<>(cause));
    }

    public static <R> Class<? super R> rowClass(Collection<Results<R>> results) {
        for (Results<R> r : results)
            return r.rowClass();
        return Object.class;
    }

    public static <R> SafeAsyncTask<List<String>> varsUnion(List<Results<R>> results) {
        SafeCompletableAsyncTask<List<String>> task = new SafeCompletableAsyncTask<>();
        AtomicInteger ready = new AtomicInteger();
        for (Results<?> r : results) {
            r.vars().whenComplete((vars, err) -> {
                if (ready.incrementAndGet() == vars.size()) {
                    LinkedHashSet<String> set = new LinkedHashSet<>();
                    for (Results<?> r2 : results)
                        set.addAll(r2.vars().orElse(emptyList()));
                    task.complete(new ArrayList<>(set));
                }
            });
        }
        return task;
    }

}
