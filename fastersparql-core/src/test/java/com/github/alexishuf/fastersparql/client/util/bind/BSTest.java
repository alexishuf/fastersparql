package com.github.alexishuf.fastersparql.client.util.bind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

}