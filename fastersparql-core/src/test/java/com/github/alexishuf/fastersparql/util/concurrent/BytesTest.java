package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class BytesTest {
    @Test void testGrow() {
        Bytes smaller = Bytes.atLeast(7).takeOwnership(this);
        Arrays.fill(smaller.arr, (byte)1);
        Bytes bigger = Bytes.grow(smaller, this, 5, 10);
        assertFalse(smaller.isAliveAndMarking());
        assertTrue(bigger.isOwnerOrNotMarking(this));
        byte[] ex = new byte[5];
        Arrays.fill(ex, (byte)1);
        assertEquals(-1, Arrays.mismatch(ex, 0, 5,
                                         bigger.arr, 0, 5));
        assertNull(bigger.recycle(this));
        assertFalse(bigger.isAliveAndMarking());
    }

}