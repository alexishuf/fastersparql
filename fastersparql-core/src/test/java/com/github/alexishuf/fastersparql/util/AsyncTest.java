package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.alexishuf.fastersparql.util.Async.waitStage;
import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class AsyncTest {

    @Test void testGetSingleThread() {
        CompletableFuture<Integer> stage = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try { Thread.sleep(10); } catch (InterruptedException ignored) { }
            stage.complete(23);
        });
        assertEquals(23, waitStage(stage));
    }

    @Test void testFailedStage() {
        CompletableFuture<Integer> stage = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try { Thread.sleep(10); } catch (InterruptedException ignored) {}
            stage.completeExceptionally(new Exception("test"));
        });
        try {
            waitStage(stage);
            fail("Expected RuntimeExecutionException");
        } catch (RuntimeExecutionException e) {
            assertEquals("test", e.getCause().getMessage());
        }
    }

    @Test void testRace() throws ExecutionException, InterruptedException {
        int threads = Math.max(1, getRuntime().availableProcessors()-1), chunk = 8192;
        List<CompletableFuture<Integer>> stages = new ArrayList<>(threads*chunk);
        for (int i = 0; i < threads*chunk; i++)
            stages.add(new CompletableFuture<>());
        List<Future<?>> tasks = new ArrayList<>();
        try (var ex = Executors.newFixedThreadPool(threads)) {
            for (int thread = 0; thread < threads; thread++) {
                int begin = thread*chunk, end = begin+chunk;
                tasks.add(ex.submit(() -> {
                    for (int i = begin; i < end; i++)
                        stages.get(i).complete(i);
                }));
            }
            tasks.add(ex.submit(() -> {
                for (int i = 0; i < stages.size(); i++)
                    assertEquals(i,  waitStage(stages.get(i)), "i="+i);
            }));
            for (Future<?> task : tasks)
                task.get();
        }
    }

}