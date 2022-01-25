package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class SafeAsyncTaskTest {
    @Test
    void testGetComplete() {
        SafeCompletableAsyncTask<Integer> task = new SafeCompletableAsyncTask<>();
        assertTrue(task.complete(23));
        assertEquals(23, task.get());
    }

    @Test
    void testGetTimeouts() throws TimeoutException {
        SafeCompletableAsyncTask<Integer> task = new SafeCompletableAsyncTask<>();
        long start = System.nanoTime();
        assertThrows(TimeoutException.class, () -> task.get(100, MILLISECONDS));
        long ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms > 75, "did not wait timeout ("+ms+"ms elapsed)");

        task.complete(23);
        start = System.nanoTime();
        assertEquals(23, task.get(200, MILLISECONDS));
        ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms < 100, "get() after complete too slow: "+ms+"ms");
    }

    @Test
    void testOrElse() {
        SafeCompletableAsyncTask<String> task = new SafeCompletableAsyncTask<>();
        assertEquals("fallback", task.orElse("fallback"));
        long start = System.nanoTime();
        assertEquals("fallback", task.orElse("fallback", 100, MILLISECONDS));
        long ms = (System.nanoTime()-start)/1000000;
        assertTrue(ms > 80, "orElse did not wait: "+ms+ "ms elapsed");
        assertTrue(ms < 200, "orElse blocked for "+ms+"ms, expected 100 ms");

        String value = "value";
        task.complete(value);
        assertSame(value, task.orElse("fallback"));
        assertSame(value, task.orElse("fallback", 100, MILLISECONDS));
    }
}