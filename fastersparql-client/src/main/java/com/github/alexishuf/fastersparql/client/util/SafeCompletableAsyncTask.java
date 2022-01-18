package com.github.alexishuf.fastersparql.client.util;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class SafeCompletableAsyncTask<T> extends CompletableAsyncTask<T>
        implements SafeAsyncTask<T> {
    @Override public T get() {
        try {
            return super.get();
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected "+e, e);
        }
    }

    @Override public T get(long timeout, @NonNull TimeUnit unit) throws TimeoutException {
        try {
            return super.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected "+e, e);
        }
    }

    @Override public boolean completeExceptionally(Throwable ex) {
        log.error("{}.completeExceptionally({}): failing a SafeAsyncTask!", this, ex);
        assert false : "completeExceptionally() on SafeAsyncTask";
        return super.completeExceptionally(ex);
    }

    @Override public boolean cancel(boolean interrupt) {
        log.warn("{}.cancel({}): denying cancellation of SafeAsyncTask", this, interrupt);
        return false;
    }
}
