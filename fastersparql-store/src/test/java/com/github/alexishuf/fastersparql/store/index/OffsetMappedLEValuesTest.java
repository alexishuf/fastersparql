package com.github.alexishuf.fastersparql.store.index;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OffsetMappedLEValuesTest {
    static Path file;
    static Arena arena;

    private static byte[] toBytes(int[] arr) {
        byte[] out = new byte[arr.length];
        for (int i = 0; i < arr.length; i++)
            out[i] = (byte)arr[i];
        return out;
    }

    @BeforeAll static void beforeAll() throws IOException {
        file = Files.createTempFile("fastersparql", "");
        arena = Arena.openShared();
        try (var ch = FileChannel.open(file, WRITE, TRUNCATE_EXISTING, CREATE)) {
            var bb = ByteBuffer.wrap(toBytes(new int[] {
                    // - 2 4BLE offsets into 8BLE values
                    // - offsets start at 0x08
                    //         00    01    02    03    04    05    06    07
                    /* 00 */ 0x02, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
                    /* 08 */ 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
                    /* 10 */ 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
                    /* 18 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    /* 20 */ 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8,
                    }));
            int ex = bb.limit();
            if (ch.write(bb) != ex) {
                throw new IOException("Did not write al bytes");
            }
        }

    }

    @AfterAll static void afterAll() throws IOException {
        arena.close();
        Files.deleteIfExists(file);
    }

    private static final class Helper extends OffsetMappedLEValues {
        private static int OFF_W = 8, VALUE_W = 8;
        public Helper() throws IOException {
            super(file, Arena.openShared());
        }

        @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
            md.offsetsOff = 8;
            md.offsetsCount = 2;
            md.offsetWidth = OFF_W;
            md.valueWidth = VALUE_W;
        }
    }

    @Test void test4BOffsets8BValues() throws Exception {
        Helper.OFF_W = 4;
        Helper.VALUE_W = 8;
        try (var h = new Helper()) {
            assertEquals(0x10, h.readOff(0));
            assertEquals(0x20, h.readOff(1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff( 2));
            assertEquals(0xa8a7a6a5a4a3a2a1L, h.readValue(0x10));
            assertEquals(0xb8b7b6b5b4b3b2b1L, h.readValue(0x20));
        }
    }

    @Test void test4BOffsets4BValues() throws Exception {
        Helper.OFF_W = 4;
        Helper.VALUE_W = 4;
        try (var h = new Helper()) {
            assertEquals(0x10, h.readOff(0));
            assertEquals(0x20, h.readOff(1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff( 2));
            assertEquals(0xa4a3a2a1L, h.readValue(0x10));
            assertEquals(0xa8a7a6a5L, h.readValue(0x14));
            assertEquals(0x00000000L, h.readValue(0x18));
            assertEquals(0xb4b3b2b1L, h.readValue(0x20));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readValue(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readValue(0x28));
        }
    }

    @Test void test4BOffsets2BValues() throws Exception {
        Helper.OFF_W = 4;
        Helper.VALUE_W = 2;
        try (var h = new Helper()) {
            assertEquals(0x10, h.readOff(0));
            assertEquals(0xa2a1L, h.readValue(0x10));
            assertEquals(0xa4a3L, h.readValue(0x12));
            assertEquals(0x0000L, h.readValue(0x18));
            assertEquals(0xb2b1L, h.readValue(0x20));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readValue(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readValue(0x28));
        }
    }


    @Test void test8BOffsets2BValues() throws Exception {
        Helper.OFF_W = 8;
        Helper.VALUE_W = 2;
        try (var h = new Helper()) {
            assertEquals(0x0000002000000010L, h.readOff(0));
            assertEquals(0xa8a7a6a5a4a3a2a1L, h.readOff(1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> h.readOff(2));
            assertEquals(0xa2a1L, h.readValue(0x10));
            assertEquals(0xa4a3L, h.readValue(0x12));
            assertEquals(0x0000L, h.readValue(0x18));
            assertEquals(0xb2b1L, h.readValue(0x20));
        }
    }
}