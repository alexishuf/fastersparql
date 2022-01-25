package com.github.alexishuf.fastersparql.client.util.async;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SafeCompletableAsyncTask<T> extends CompletableAsyncTask<T>
        implements SafeAsyncTask<T> {
    private static final Logger log = LoggerFactory.getLogger(SafeCompletableAsyncTask.class);

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
