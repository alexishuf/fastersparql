package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.Vars;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class LazyCallbackBItTest extends AbstractBItTest {

    @Override protected List<? extends Scenario> scenarios() {
        return baseScenarios();
    }

    @Override protected void run(Scenario s) {
        CompletableFuture<Thread> workerThread = new CompletableFuture<>();
        var it = new LazyCallbackBIt<>(Integer.class, Vars.EMPTY) {
            @Override protected void run() {
                workerThread.complete(Thread.ofVirtual().start(() -> {
                    for (int i = 0; i < s.size; i++)
                        feed(i);
                    complete(s.error);
                }));
            }
        };
        s.drainer().drainOrdered(it, s.expected(), s.error);
        try {
            assertTrue(workerThread.get().join(Duration.ofMillis(100)), "workerThread still alive");
        } catch (InterruptedException|ExecutionException e) {
            fail(e);
        }
    }


}