package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.AsyncIterableCancelled;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class IterableAdapterTest {
    @Test
    void testMonoBigQueue() {
        IterableAdapter<Integer> adapter = new IterableAdapter<>(Mono.just(1), 1024);
        Iterator<Integer> it = adapter.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void testMonoUnitQueue() {
        IterableAdapter<Integer> adapter = new IterableAdapter<>(Mono.just(1), 1);
        Iterator<Integer> it = adapter.iterator();
        assertTrue(it.hasNext());
        assertEquals(it.next(), 1);
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void testSubscribeBeforeConsumption() {
        IterableAdapter<Integer> adapter = new IterableAdapter<>(Mono.just(1), 1);
        assertSame(adapter, adapter.start());
        Iterator<Integer> it = adapter.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    void testIdempotentSubscribe() {
        IterableAdapter<Integer> adapter = new IterableAdapter<>(Mono.just(1), 10);
        for (int i = 0; i < 10; i++)
            assertSame(adapter, adapter.start());
        Iterator<Integer> it = adapter.iterator();
        for (int i = 0; i < 10; i++)
            assertSame(adapter, adapter.start());
        assertTrue(it.hasNext());
        for (int i = 0; i < 10; i++)
            assertSame(adapter, adapter.start());
        assertEquals(1, it.next());
        for (int i = 0; i < 10; i++)
            assertSame(adapter, adapter.start());
        assertFalse(it.hasNext());
    }

    @Test
    void testIteratorAfterClose() {
        IterableAdapter<Integer> adapter = new IterableAdapter<>(Mono.just(1), 10);
        adapter.close();
        assertThrows(IllegalStateException.class, adapter::iterator);
    }

    @Test
    void testCancelBeforeStart() {
        IterableAdapter<Integer> a = new IterableAdapter<>(Mono.just(1), 10);
        a.cancel();
        assertTrue(a.error() instanceof AsyncIterableCancelled);
    }

    @ParameterizedTest @ValueSource(strings = {
            "1    | false | false",
            "2    | false | false",
            "3    | false | false",
            "4    | false | false",
            "8    | false | false",
            "32   | false | false",
            "8192 | false | false",

            "1    | true | false",
            "2    | true | false",
            "3    | true | false",
            "4    | true | false",
            "8    | true | false",
            "32   | true | false",
            "8192 | true | false",

            "1    | false | true",
            "2    | false | true",
            "3    | false | true",
            "4    | false | true",
            "8    | false | true",
            "32   | false | true",
            "8192 | false | true",

            "1    | true | true",
            "8192 | true | true",
    })
    void testCancelAfterStart(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int size = Integer.parseInt(data[0]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[1]);
        boolean offloadPublish = Boolean.parseBoolean(data[2]);
        Flux<Integer> flux = Flux.range(1, size + 1);
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());
        IterableAdapter<Integer> a = new IterableAdapter<>(flux, 16);
        a.start();
        a.cancel();
        assertTrue(a.error() instanceof AsyncIterableCancelled);
        assertThrows(IllegalStateException.class, a::iterator);
        assertThrows(IllegalStateException.class, a::spliterator);
        assertThrows(IllegalStateException.class, a::stream);
        assertTrue(a.error() instanceof AsyncIterableCancelled);
    }

    @Test
    void testTwoIterators() {
        IterableAdapter<Integer> a = new IterableAdapter<>(Mono.just(1), 10);
        Iterator<Integer> it = a.iterator();
        assertThrows(IllegalStateException.class, a::iterator);
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        assertThrows(IllegalStateException.class, a::iterator);
    }

    @Test
    void testCancelBeforeSubscribe() {
        IterableAdapter<Integer> a = new IterableAdapter<>(Mono.just(1), 10);
        a.close();
        assertThrows(IllegalStateException.class, a::iterator);
        assertThrows(IllegalStateException.class, a::stream);
        assertThrows(IllegalStateException.class, a::spliterator);
    }

    @ParameterizedTest @ValueSource(strings = {
            "1     | 1    | false | false",
            "2     | 1    | false | false",
            "4     | 1    | false | false",
            "16    | 1    | false | false",
            "1024  | 1    | false | false",
            "8192  | 1    | false | false",
            "65536 | 1    | false | false",
            "1024  | 2    | false | false",
            "1024  | 3    | false | false",
            "1024  | 4    | false | false",
            "1024  | 5    | false | false",
            "4096  | 16   | false | false",
            "4096  | 256  | false | false",
            "4096  | 1024 | false | false",

            "1     | 1    | true | false",
            "2     | 1    | true | false",
            "4     | 1    | true | false",
            "16    | 1    | true | false",
            "1024  | 1    | true | false",
            "8192  | 1    | true | false",
            "65536 | 1    | true | false",
            "1024  | 2    | true | false",
            "1024  | 3    | true | false",
            "1024  | 4    | true | false",
            "1024  | 5    | true | false",
            "4096  | 16   | true | false",
            "4096  | 256  | true | false",
            "4096  | 1024 | true | false",

            "1     | 1    | false | true",
            "2     | 1    | false | true",
            "4     | 1    | false | true",
            "16    | 1    | false | true",
            "1024  | 1    | false | true",
            "8192  | 1    | false | true",
            "65536 | 1    | false | true",
            "1024  | 2    | false | true",
            "1024  | 3    | false | true",
            "1024  | 4    | false | true",
            "1024  | 5    | false | true",
            "4096  | 16   | false | true",
            "4096  | 256  | false | true",
            "4096  | 1024 | false | true",

            "1     | 1    | true | true",
            "2     | 1    | true | true",
            "4096  | 1    | true | true",
            "4096  | 1024 | true | true"
    })
    void testCancelAfterSubscribeBeforeIterator(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int size = Integer.parseInt(data[0]), capacity = Integer.parseInt(data[1]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[2]);
        boolean offloadPublish = Boolean.parseBoolean(data[3]);

        Flux<Integer> flux = Flux.range(0, size);
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());
        IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity);
        assertSame(a, a.start());
        a.close();
        assertThrows(IllegalStateException.class, a::iterator);
        assertThrows(IllegalStateException.class, a::stream);
        assertThrows(IllegalStateException.class, a::spliterator);
    }

    @ParameterizedTest @ValueSource(strings = {
            "1     | 1     | false | false",
            "2     | 1     | false | false",
            "4     | 1     | false | false",
            "16    | 1     | false | false",
            "1024  | 1     | false | false",
            "8192  | 1     | false | false",
            "65536 | 1     | false | false",
            "65536 | 1     | false | true",
            "1024  | 2     | false | false",
            "1024  | 3     | false | false",
            "1024  | 4     | false | false",
            "1024  | 5     | false | false",
            "4096  | 16    | false | false",
            "4096  | 256   | false | false",
            "4096  | 1024  | false | false",
            "4096  | 16    | false | true",
            "4096  | 1024  | false | true",

            "1     | 1     | true  | false",
            "2     | 1     | true  | false",
            "4     | 1     | true  | false",
            "16    | 1     | true  | false",
            "1024  | 1     | true  | false",
            "8192  | 1     | true  | false",
            "65536 | 1     | true  | false",
            "65536 | 1     | true  | true",
            "1024  | 2     | true  | false",
            "1024  | 3     | true  | false",
            "1024  | 4     | true  | false",
            "1024  | 5     | true  | false",
            "4096  | 16    | true  | false",
            "4096  | 256   | true  | false",
            "4096  | 1024  | true  | false",
            "4096  | 16    | true  | true",
            "4096  | 1024  | true  | true",

            "1     | 1     | false | true",
            "2     | 1     | false | true",
            "4     | 1     | false | true",
            "16    | 1     | false | true",
            "1024  | 1     | false | true",
            "8192  | 1     | false | true",
            "65536 | 1     | false | true",
            "1024  | 2     | false | true",
            "1024  | 3     | false | true",
            "1024  | 4     | false | true",
            "1024  | 5     | false | true",
            "4096  | 16    | false | true",
            "4096  | 256   | false | true",
            "4096  | 1024  | false | true",
    })
    void testCancelAfterFirstNext(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int size = Integer.parseInt(data[0]), capacity = Integer.parseInt(data[1]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[2]);
        boolean offloadPublish = Boolean.parseBoolean(data[3]);

        Flux<Integer> flux = Flux.range(1, size + 1);
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());
        IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity);
        assertSame(a, a.start());
        Iterator<Integer> it = a.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        a.close();
        for (int i = 2; it.hasNext() && i <= size; i++)
            assertEquals(i, it.next());
    }

    @ParameterizedTest @ValueSource(strings = {
            "1    | 1    | false | false",
            "2    | 1    | false | false",
            "3    | 1    | false | false",
            "4    | 1    | false | false",
            "8    | 1    | false | false",
            "8192 | 1    | false | false",
            "8    | 2    | false | false",
            "8    | 3    | false | false",
            "8    | 4    | false | false",
            "8    | 5    | false | false",
            "8    | 8    | false | false",
            "1    | 1024 | false | false",
            "4    | 8192 | false | false",

            "1    | 1    | false | true",
            "2    | 1    | false | true",
            "3    | 1    | false | true",
            "4    | 1    | false | true",
            "8    | 1    | false | true",
            "8192 | 1    | false | true",
            "8    | 2    | false | true",
            "8    | 3    | false | true",
            "8    | 4    | false | true",
            "8    | 5    | false | true",
            "8    | 8    | false | true",
            "1    | 1024 | false | true",
            "4    | 8192 | false | true",

            "1    | 1    | true  | false",
            "2    | 1    | true  | false",
            "3    | 1    | true  | false",
            "4    | 1    | true  | false",
            "8    | 1    | true  | false",
            "8192 | 1    | true  | false",
            "8    | 2    | true  | false",
            "8    | 3    | true  | false",
            "8    | 4    | true  | false",
            "8    | 5    | true  | false",
            "8    | 8    | true  | false",
            "1    | 1024 | true  | false",
            "4    | 8192 | true  | false",

            "8192 | 1    | true  | true",
            "4    | 8192 | true  | true",
    })
    void testInfinitePublisher(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int size = Integer.parseInt(data[0]), capacity = Integer.parseInt(data[1]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[2]);
        boolean offloadPublish = Boolean.parseBoolean(data[3]);

        Flux<Integer> flux = Flux.generate(new Consumer<SynchronousSink<Integer>>() {
            private int next = 1;

            @Override public void accept(SynchronousSink<Integer> synchronousSink) {
                synchronousSink.next(next++);
            }
        });
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());

        List<Integer> expected = IntStream.range(1, size+1).boxed().collect(Collectors.toList());
        List<Integer> actual = new ArrayList<>();
        try (IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity)) {
            Iterator<Integer> it = a.iterator();
            for (int i = 0; i < size; i++) {
                assertTrue(it.hasNext());
                actual.add(it.next());
            }
        }
        assertEquals(expected, actual);
    }

    @Test
    void testIterateEmpty() {
        try (IterableAdapter<Integer> a = new IterableAdapter<>(Mono.empty(), 10)) {
            for (int val : a)
                fail();
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach") @ParameterizedTest @ValueSource(strings = {
            "0    | 1    | false | false",
            "1    | 1    | false | false",
            "2    | 1    | false | false",
            "3    | 1    | false | false",
            "4    | 1    | false | false",
            "8    | 1    | false | false",
            "16   | 1    | false | false",
            "128  | 1    | false | false",
            "1024 | 1    | false | false",
            "8192 | 1    | false | false",
            "2    | 2    | false | false",
            "4    | 2    | false | false",
            "4    | 3    | false | false",
            "4    | 4    | false | false",
            "8    | 2    | false | false",
            "8    | 4    | false | false",
            "8    | 8    | false | false",
            "16   | 2    | false | false",
            "16   | 4    | false | false",
            "16   | 8    | false | false",
            "16   | 7    | false | false",
            "16   | 3    | false | false",
            "16   | 15   | false | false",
            "8192 | 10   | false | false",
            "8192 | 128  | false | false",
            "8192 | 4096 | false | false",
            "8192 | 8192 | false | false",

            "0    | 1    | true  | false",
            "1    | 1    | true  | false",
            "2    | 1    | true  | false",
            "3    | 1    | true  | false",
            "4    | 1    | true  | false",
            "8    | 1    | true  | false",
            "16   | 1    | true  | false",
            "128  | 1    | true  | false",
            "1024 | 1    | true  | false",
            "8192 | 1    | true  | false",
            "2    | 2    | true  | false",
            "4    | 2    | true  | false",
            "4    | 3    | true  | false",
            "4    | 4    | true  | false",
            "8    | 2    | true  | false",
            "8    | 4    | true  | false",
            "8    | 8    | true  | false",
            "16   | 2    | true  | false",
            "16   | 4    | true  | false",
            "16   | 8    | true  | false",
            "16   | 7    | true  | false",
            "16   | 3    | true  | false",
            "16   | 15   | true  | false",
            "8192 | 10   | true  | false",
            "8192 | 128  | true  | false",
            "8192 | 4096 | true  | false",
            "8192 | 8192 | true  | false",

            "1    | 1    | true  | true",
            "1    | 128  | true  | true",
            "4    | 1    | true  | true",
            "4    | 4    | true  | true",
            "8192 | 1    | true  | true",
            "8192 | 128  | true  | true",
            "8192 | 8192 | true  | true",

            "0    | 1    | false | true",
            "1    | 1    | false | true",
            "2    | 1    | false | true",
            "3    | 1    | false | true",
            "4    | 1    | false | true",
            "8    | 1    | false | true",
            "16   | 1    | false | true",
            "128  | 1    | false | true",
            "1024 | 1    | false | true",
            "8192 | 1    | false | true",
            "2    | 2    | false | true",
            "4    | 2    | false | true",
            "4    | 3    | false | true",
            "4    | 4    | false | true",
            "8    | 2    | false | true",
            "8    | 4    | false | true",
            "8    | 8    | false | true",
            "16   | 2    | false | true",
            "16   | 4    | false | true",
            "16   | 8    | false | true",
            "16   | 7    | false | true",
            "16   | 3    | false | true",
            "16   | 15   | false | true",
            "8192 | 10   | false | true",
            "8192 | 128  | false | true",
            "8192 | 4096 | false | true",
            "8192 | 8192 | false | true",
    })
    void testConsumeAll(String dataString) {
        String[] data = dataString.split("\\s*\\|\\s*");
        int size = Integer.parseInt(data[0]), capacity = Integer.parseInt(data[1]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[2]);
        boolean offloadPublish = Boolean.parseBoolean(data[3]);

        List<Integer> expected = IntStream.range(0, size).boxed().collect(Collectors.toList());
        Flux<Integer> flux = Flux.fromIterable(expected);
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());

        List<Integer> iterated = new ArrayList<>();
        List<Integer> loop = new ArrayList<>();
        List<Integer> forEach = new ArrayList<>();
        List<Integer> streamed = new ArrayList<>();
        try (IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity)) {
            for (Iterator<Integer> it = a.iterator(); it.hasNext(); )
                iterated.add(it.next());
        }
        try (IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity)) {
            for (Integer i : a) loop.add(i);
        }
        try (IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity)) {
            a.forEach(forEach::add);
        }
        try (IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity)) {
            a.stream().forEach(streamed::add);
        }

        assertEquals(iterated, expected);
        assertEquals(loop, expected);
        assertEquals(forEach, expected);
        assertEquals(streamed, expected);
    }

    @ParameterizedTest @ValueSource(strings = {
            "1  | 1  | false | false",
            "2  | 1  | false | false",
            "4  | 1  | false | false",
            "16 | 1  | false | false",
            "32 | 1  | false | false",
            "1  | 2  | false | false",
            "2  | 2  | false | false",
            "4  | 2  | false | false",
            "16 | 2  | false | false",
            "32 | 2  | false | false",
            "16 | 4  | false | false",
            "32 | 4  | false | false",
            "16 | 8  | false | false",
            "32 | 8  | false | false",
            "32 | 32 | false | false",

            "1  | 1  | true  | false",
            "2  | 1  | true  | false",
            "4  | 1  | true  | false",
            "16 | 1  | true  | false",
            "32 | 1  | true  | false",
            "1  | 2  | true  | false",
            "2  | 2  | true  | false",
            "4  | 2  | true  | false",
            "16 | 2  | true  | false",
            "32 | 2  | true  | false",
            "16 | 4  | true  | false",
            "32 | 4  | true  | false",
            "16 | 8  | true  | false",
            "32 | 8  | true  | false",
            "32 | 32 | true  | false",

            "1  | 1  | false | true",
            "2  | 1  | false | true",
            "4  | 1  | false | true",
            "16 | 1  | false | true",
            "32 | 1  | false | true",
            "1  | 2  | false | true",
            "2  | 2  | false | true",
            "4  | 2  | false | true",
            "16 | 2  | false | true",
            "32 | 2  | false | true",
            "16 | 4  | false | true",
            "32 | 4  | false | true",
            "16 | 8  | false | true",
            "32 | 8  | false | true",
            "32 | 32 | false | true",

            "1  | 64 | true | true",
            "16 | 8  | true | true",
            "32 | 8  | true | true",
            "32 | 32 | true | true",
    })
    void testCancelMidway(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int size = Integer.parseInt(data[0]), capacity = Integer.parseInt(data[1]);
        boolean offloadSubscribe = Boolean.parseBoolean(data[2]);
        boolean offloadPublish = Boolean.parseBoolean(data[3]);

        for (int i = 0; i < 10; i++)
            doTestCancelMidway(size, capacity, offloadSubscribe, offloadPublish);
    }

    private void doTestCancelMidway(int size, int capacity, boolean offloadSubscribe,
                                    boolean offloadPublish) {
        Flux<Integer> flux = Flux.generate(new Consumer<SynchronousSink<Integer>>() {
            int next = 1;
            @Override public void accept(SynchronousSink<Integer> sink) {sink.next(next++);}
        });
        if (offloadSubscribe)
            flux = flux.subscribeOn(Schedulers.boundedElastic());
        if (offloadPublish)
            flux = flux.publishOn(Schedulers.boundedElastic());

        List<Integer> expected = IntStream.range(1, size+1).boxed().collect(Collectors.toList());
        List<Integer> actual = new ArrayList<>();
        IterableAdapter<Integer> a = new IterableAdapter<>(flux, capacity);
        Iterator<Integer> it = a.iterator();
        for (int i = 0; i < size; i++) {
            assertTrue(it.hasNext());
            actual.add(it.next());
        }
        a.cancel();
        assertTrue(a.error() instanceof AsyncIterableCancelled);
        assertTimeout(Duration.ofSeconds(1), () -> {while (it.hasNext()) it.next();});
        assertFalse(it.hasNext());
        assertTrue(a.error() instanceof AsyncIterableCancelled);
        assertEquals(expected, actual);
    }
}