package com.github.alexishuf.fastersparql.util;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class IntListTest {
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static ExecutorService executor;

    @BeforeAll static void beforeAll() {
        executor = Executors.newFixedThreadPool(THREADS);
    }

    @AfterAll static void afterAll() throws InterruptedException {
        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    }

    static Stream<Arguments> test() {
        return Stream.concat(
                Stream.of(1, 2, 3, 15, 16, 17, 30, 31, 32, 33, 34, 48, 63, 64, 65,
                          96, 112, 126, 127, 128, 128, 130, 200, 250, 255, 256, 257,
                          1_000, 1023, 1024, 1025, 2_000, 2047, 2048, 2049, 8191, 8192,
                          8193
                ).map(size -> arguments(16, size)),
                Stream.of(1, 63, 127, 128, 129, 8191, 8192, 8193).map(size -> arguments(127, size))
        );
    }

    @ParameterizedTest @MethodSource
    void test(int blockSize, int size) throws ExecutionException, InterruptedException {
        IntList list = new IntList(blockSize);
        doTest(list, size);
        list.clear();
        doTest(list, size);
        ArrayList<Future<?>> tasks = new ArrayList<>(THREADS);
        tasks.add(executor.submit(() -> doTest(new IntList(blockSize), size)));
        for (Future<?> t : tasks)
            t.get();
    }

    private void doTest(IntList list, int size) {
        IntList other = new IntList(16);
        IntList.It persistentIt = list.iterator();
        for (int i = 0; i < size; i++) {
            assertEquals(-1, list.indexOf(i));
            assertFalse(list.contains(i));
            assertTrue(list.add(i));
            assertEquals(i+1, list.size());
            assertTrue(list.contains(i));
            assertEquals(i, list.get(i));
            assertEquals(i, list.indexOf(i));
            assertEquals(i, list.removeLastInt());
            assertEquals(i, list.size());
            assertTrue(list.add(i));
            assertTrue(list.add(i));
            assertEquals(i+2, list.size());
            assertEquals(i, list.remove(i));
            assertEquals(i+1, list.size());
            assertEquals(i, list.getLast());
            for (int step = 4; step <= 64 && i >= step; step <<= 1)
                assertEquals(i-step, list.get(i-step));
            assertTrue(persistentIt.hasNext());
            assertEquals(i, persistentIt.next());
            other.add(i);
            assertEquals(other, list);
        }
        assertEquals(size, list.size());
        var fwdIt = list.iterator();
        var bwdIt = list.listIterator(size);
        for (int i = 0; i < size; i++) {
            assertTrue(fwdIt.hasNext());
            assertEquals(i, fwdIt.next());
            assertTrue(bwdIt.hasPrevious());
            assertEquals(size-1-i, bwdIt.previous());
        }
    }

    @Test
    void testFillAndIterate() {
        IntList list = new IntList(16);
        for (int i = 0; i < 64; i++)
            list.add(i);
        for (int i = 0; i < 64; i++)
            assertEquals(i, list.get(i));
        IntList.It it = list.iterator();
        for (int i = 0; i < 64; i++) {
            assertTrue(it.hasNext());
            assertEquals(i, it.next());
        }
    }


    @Test void testClear() {
        IntList list = new IntList(16);
        for (int i = 0; i < 64; i++)
            list.add(i);
        list.clear();
        //noinspection ConstantValue
        assertEquals(0, list.size());
        assertTrue(list.isEmpty());
    }
    
    @SuppressWarnings({"SimplifiableAssertion", "EqualsWithItself"}) @Test
    void testEquality() {
        IntList abc = new IntList(16), abc2 = new IntList(16), abc3 = new IntList(32);
        IntList a = new IntList(16), b = new IntList(16), c = new IntList(32);
        IntList cba = new IntList(16), cba2 = new IntList(32);
        List<Integer> boxedABC = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            abc .add(i);
            abc2.add(i);
            abc3.add(i);
            boxedABC.add(i);
        }
        for (int i = 31; i >= 0; --i) {
            cba .add(i);
            cba2.add(i);
        }
        for (int i = 0;  i < 16; i++) a.add(i);
        for (int i = 16; i < 24; i++) b.add(i);
        for (int i = 24; i < 32; i++) c.add(i);

        // reflexive
        assertTrue(abc.equals(abc));
        assertTrue(abc2.equals(abc2));
        assertTrue(abc3.equals(abc3));
        assertTrue(cba.equals(cba));
        assertTrue(cba2.equals(cba2));
        assertTrue(a.equals(a));
        assertTrue(b.equals(b));
        assertTrue(c.equals(c));

        // equality
        assertEquals(abc, abc2);
        assertEquals(abc, abc3);
        assertEquals(cba, cba2);

        //equality with boxed
        assertEquals(abc, boxedABC);
        assertEquals(abc2, boxedABC);
        assertEquals(abc3, boxedABC);

        // symmetric
        assertEquals(abc2, abc);
        assertEquals(abc3, abc);
        assertEquals(cba2, cba);
        assertEquals(boxedABC, abc);
        assertEquals(boxedABC, abc2);
        assertEquals(boxedABC, abc3);

        // size mismatch
        assertNotEquals(abc, a  );
        assertNotEquals(b,   abc);
        assertNotEquals(abc, c  );
        assertNotEquals(c,   cba);

        // non-equals, same size
        assertNotEquals(b, c);
        assertNotEquals(c, b);
        assertNotEquals(abc, cba);
        assertNotEquals(cba2, abc2);
        assertNotEquals(abc3, cba);
        assertNotEquals(cba2, abc3);
        assertNotEquals(boxedABC, cba);
        assertNotEquals(boxedABC, cba2);
        assertNotEquals(cba, boxedABC);
        assertNotEquals(cba2, boxedABC);
    }
    

    @Test void testAppend() {
        IntList list = new IntList(16);
        for (int i = 0; i < 64; i++) {
            assertEquals(i, list.size());
            assertFalse(list.contains(i));
            if ((i&1) == 0)
                list.add(i, i);
            else
                list.addLast(i);
            assertEquals(i+1, list.size());
            assertEquals(i, list.indexOf(i));
            assertTrue(list.contains(i));
            for (int j = 0; j < i; j++) {
                assertEquals(j, list.indexOf(j));
                assertTrue(list.contains(j));
            }
        }
        assertEquals(64, list.size());
    }

    @Test void testPrepend() {
        IntList list = new IntList(16);
        for (int i = 63; i >= 0; --i) {
            if ((i&1) == 0)
                list.add(0, i);
            else
                list.addFirst(i);
        }
        assertEquals(64, list.size());
        for (int i = 0; i < 64; i++)
            assertEquals(i, list.get(i));
    }

    @Test void testIteratorRemove() {
        IntList list = new IntList(16);
        for (int i = 0; i < 64; i++)
            list.add(i);
        var it = list.iterator();
        for (int i = 0; i < 64; i++) {
            assertTrue(it.hasNext());
            assertEquals(i, it.next());
            it.remove();
        }
        assertFalse(it.hasNext());
        assertFalse(list.iterator().hasNext());
        assertTrue(list.isEmpty());
    }

    @Test void testIteratorAdd() {
        IntList list = new IntList(16);
        for (int i = 0; i < 64; i += 2)
            list.add(i);
        assertEquals(32, list.size());
        var it = list.iterator();
        for (int expected = 0; it.hasNext(); expected += 2) {
            assertEquals(expected, it.next());
            it.add(expected+1);
        }
        assertEquals(64, list.size());
        var revIt = list.listIterator(list.size());
        for (int expected = 63; revIt.hasPrevious(); --expected) {
            assertTrue(expected >= 0);
            assertEquals(expected, revIt.previous());
        }
        assertEquals(64, list.size());
    }


}