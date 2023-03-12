package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.util.BS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class BSTest {
    static Stream<Arguments> sizes() {
        return Stream.of(1, 31, 32, 48, 63, 64, 96, 127, 128).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("sizes")
    void testSet(int size) {
        for (int i = 0; i < size; i++) {
            int[] set = BS.init(null, size);
            BS.set(set, 0, i);
            for (int j = 0; j < i; j++)
                assertEquals(1<<j, set[j>>5] & (1 << j));
            for (int j = i; j < size; j++)
                assertEquals(0, set[j>>5] & (1 << j));
        }
    }

    @ParameterizedTest @MethodSource("sizes")
    void testClear(int size) {
        for (int i = 0; i < size; i++) {
            int[] set = BS.init(null, size);
            BS.set(set, 0, size);
            BS.clear(set, 0, i);
            for (int j = 0; j < i; j++)
                assertEquals(0, set[j>>5] & (1 << j));
            for (int j = i; j < size; j++)
                assertEquals(1 << j, set[j>>5] & (1 << j));
        }
    }

    @Test
    void testSetAndClearOneWord() {
        int[] set = BS.init(null, 64);
        BS.set(set, 8, 24);
        assertEquals(16, BS.cardinality(set));
        assertEquals(0x00ffff00, set[0]);
        assertEquals(0x00000000, set[1]);

        BS.set(set, 0, 64);
        assertEquals(64, BS.cardinality(set));
        assertEquals(0xffffffff, set[0]);
        assertEquals(0xffffffff, set[1]);

        BS.clear(set, 8, 24);
        assertEquals(48, BS.cardinality(set));
        assertEquals(0xff0000ff, set[0]);
        assertEquals(0xffffffff, set[1]);
    }

    @Test
    void testSetAndClearTwoWords() {
        int[] set = BS.init(null, 64);
        BS.set(set, 24, 40);
        assertEquals(16, BS.cardinality(set));
        assertEquals(0xff000000, set[0]);
        assertEquals(0x000000ff, set[1]);

        BS.set(set, 0, 64);
        assertEquals(64, BS.cardinality(set));
        assertEquals(0xffffffff, set[0]);
        assertEquals(0xffffffff, set[1]);

        BS.clear(set, 24, 40);
        assertEquals(48, BS.cardinality(set));
        assertEquals(0x00ffffff, set[0]);
        assertEquals(0xffffff00, set[1]);
    }

    @Test
    void testSetAndClearThreeWords() {
        int[] set = BS.init(null, 96);
        BS.set(set, 24, 72);
        assertEquals(48, BS.cardinality(set));
        assertEquals(0xff000000, set[0]);
        assertEquals(0xffffffff, set[1]);
        assertEquals(0x000000ff, set[2]);

        BS.set(set, 0, 96);
        assertEquals(96, BS.cardinality(set));
        assertEquals(0xffffffff, set[0]);
        assertEquals(0xffffffff, set[1]);
        assertEquals(0xffffffff, set[2]);

        BS.clear(set, 24, 72);
        assertEquals(48, BS.cardinality(set));
        assertEquals(0x00ffffff, set[0]);
        assertEquals(0x00000000, set[1]);
        assertEquals(0xffffff00, set[2]);
    }

    @ParameterizedTest @MethodSource("sizes")
    void testSingleBit(int size) {
        for (int i = 0; i < size; i++) {
            int[] bs = BS.init(null, size);
            assertNotNull(bs);
            assertTrue(bs.length > (size-1)>>5);
            assertSame(bs, BS.init(bs, size));

            bs[i>>5] |= 1 << i;
            assertEquals(i, BS.nextSet(bs, 0));
            assertEquals(i, BS.nextSet(bs, i));
            assertEquals(-1, BS.nextSet(bs, i+1), "i="+i);

            assertEquals(i == 0 ? 1 : 0, BS.nextClear(bs, 0), "i="+i);
            assertEquals(i+1, BS.nextClear(bs, i), "i="+i);
            assertEquals(i+1, BS.nextClear(bs, i+1));

            assertEquals(1, BS.cardinality(bs));
            assertSame(bs, BS.init(bs, size));
            assertEquals(0, BS.cardinality(bs));
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 4,
            31, 32, 33,
            63, 64, 65,
            127, 128, 129,
            191, 192, 193,
            255, 256, 257})
    void test(int size) {
        long[] lSet = new long[BS.longsFor(size)];
        int [] iSet = new  int[BS. intsFor(size)];
        if (size == 0) return;

        BS.set(lSet, 0); BS.set(lSet, size-1);
        BS.set(iSet, 0); BS.set(iSet, size-1);
        assertTrue(BS.get(iSet, 0));
        assertTrue(BS.get(lSet, 0));
        assertTrue(BS.get(iSet, size-1));
        assertTrue(BS.get(lSet, size-1));
        assertEquals(Math.min(size, 2), BS.cardinality(iSet));
        assertEquals(Math.min(size, 2), BS.cardinality(lSet));

        BS.clear(lSet, 0); BS.clear(lSet, size-1);
        BS.clear(iSet, 0); BS.clear(iSet, size-1);
        assertFalse(BS.get(iSet, 0));
        assertFalse(BS.get(lSet, 0));
        assertFalse(BS.get(iSet, size-1));
        assertFalse(BS.get(lSet, size-1));
        assertEquals(0, BS.cardinality(iSet));
        assertEquals(0, BS.cardinality(lSet));

        long[] lAll = new long[lSet.length], lNone = new long[lSet.length];
        int [] iAll = new int [iSet.length], iNone = new int [iSet.length];
        long[] lTmp = new long[lSet.length];
        int [] iTmp = new int [iSet.length];
        Arrays.fill(lAll, -1);
        Arrays.fill(iAll, -1);

        BS.negate(lTmp, lSet);
        assertArrayEquals(lAll, lTmp);
        assertArrayEquals(lNone, lSet);
        BS.negate(iTmp, iSet);
        assertArrayEquals(iAll, iTmp);
        assertArrayEquals(iNone, iSet);

        BS.andNot(lTmp, lSet);
        assertArrayEquals(lAll, lTmp);
        BS.andNot(lTmp, lAll);
        assertArrayEquals(lNone, lTmp);

        BS.andNot(iTmp, iSet);
        assertArrayEquals(iAll, iTmp);
        BS.andNot(iTmp, iAll);
        assertArrayEquals(iNone, iTmp);

        BS.copy(lTmp, lAll);
        assertArrayEquals(lAll, lTmp);
        BS.and(lTmp, lSet);
        assertArrayEquals(lNone, lTmp);

        BS.copy(iTmp, iAll);
        assertArrayEquals(iAll, iTmp);
        BS.and(iTmp, iSet);
        assertArrayEquals(iNone, iTmp);

        BS.or(lTmp, lAll);
        assertArrayEquals(lAll, lTmp);
        BS.or(lTmp, lSet);
        assertArrayEquals(lAll, lTmp);

        BS.or(iTmp, iAll);
        assertArrayEquals(iAll, iTmp);
        BS.or(iTmp, iSet);
        assertArrayEquals(iAll, iTmp);

        BS.copy(lTmp, lNone);
        BS.orAnd(lTmp, lAll, lSet);
        assertArrayEquals(lNone, lTmp);
        BS.andNot(lTmp, lSet);
        assertArrayEquals(lNone, lTmp);
        BS.orAnd(lTmp, lTmp, lAll);
        assertArrayEquals(lNone, lTmp);
        BS.orAnd(lTmp, lAll, lAll);
        assertArrayEquals(lAll, lTmp);

        BS.copy(iTmp, iNone);
        BS.orAnd(iTmp, iAll, iSet);
        assertArrayEquals(iNone, iTmp);
        BS.andNot(iTmp, iSet);
        assertArrayEquals(iNone, iTmp);
        BS.orAnd(iTmp, iTmp, iAll);
        assertArrayEquals(iNone, iTmp);
        BS.orAnd(iTmp, iAll, iAll);
        assertArrayEquals(iAll, iTmp);

        BS.copy(lSet, lNone);
        BS.copy(iSet, iNone);

        List<Integer> indexes = new ArrayList<>(), expectedIndexes = new ArrayList<>();
        for (int i = 0; i < size; i += 16) {
            BS.set(iSet, i);
            BS.set(lSet, i);
            expectedIndexes.add(i);
        }

        for (int i = 0; (i = BS.nextSet(lSet, i)) != -1 ; i++) indexes.add(i);
        assertEquals(expectedIndexes, indexes);
        indexes.clear();
        for (int i = 0; (i = BS.nextSet(iSet, i)) != -1 ; i++) indexes.add(i);
        assertEquals(expectedIndexes, indexes);

        indexes.clear();
        for (int i = 0; (i = BS.nextSetOrLen(lSet, i)) < size ; i++)
            indexes.add(i);
        assertEquals(expectedIndexes, indexes);
        indexes.clear();
        for (int i = 0; (i = BS.nextSetOrLen(iSet, i)) < size ; i++) indexes.add(i);
        assertEquals(expectedIndexes, indexes);

        BS.negate(lSet, lSet);
        BS.negate(iSet, iSet);

        indexes.clear();
        for (int i = 0; (i = BS.nextClear(lSet, i)) < size ; i++) indexes.add(i);
        assertEquals(expectedIndexes, indexes);
        indexes.clear();
        for (int i = 0; (i = BS.nextClear(iSet, i)) < size ; i++) indexes.add(i);
        assertEquals(expectedIndexes, indexes);

        BS.negate(lSet, lSet);
        BS.negate(iSet, iSet);
        for (int end = 0; end <= size; end += 16) {
            int expected = 0;
            for (int i : expectedIndexes) { if (i < end) expected++; }
            assertEquals(expected, BS.cardinality(lSet, 0, end));
            assertEquals(expected, BS.cardinality(iSet, 0, end));
        }
    }

    @Test void testRangeGet() {
        long[] lTrivial = new long[] {0x1a2a3a4a1f2f3f4fL};
        int [] iTrivial = new int [] {0x1a2a3a4a};
        assertEquals(lTrivial[0],         BS.get(lTrivial, 0, 64));
        assertEquals(0x000000001f2f3f4fL, BS.get(lTrivial, 0, 32));
        assertEquals(0x000000001a2a3a4aL, BS.get(lTrivial, 32, 64));
        assertEquals(iTrivial[0], BS.get(iTrivial, 0, 32));
        assertEquals(0x00003a4a,  BS.get(iTrivial, 0, 16));
        assertEquals(0x00001a2a,  BS.get(iTrivial, 16, 32));


        long[] lSplit = new long[] {0x1a2a3a4a1f2f3f4fL, 0x1b2b3b4b1e2e3e4eL};
        int [] iSplit = new int [] {0x1a2a3a4a, 0x1b2b3b4b, 0x1c2c3c4c};
        assertEquals(0x1e2e3e4e1a2a3a4aL, BS.get(lSplit, 32, 96));
        assertEquals(0x000000003b4b1a2aL, BS.get(iSplit, 16, 48));
        assertEquals(0x3c4c1b2b3b4b1a2aL, BS.get(iSplit, 16, 80));
    }
}