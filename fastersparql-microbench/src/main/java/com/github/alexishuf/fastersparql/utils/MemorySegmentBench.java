package com.github.alexishuf.fastersparql.utils;

import org.openjdk.jmh.annotations.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 100, time = 10, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MemorySegmentBench {
    private static final int SIZE = 32*1024*1024;
    private static final int N_JUMPS = 8;
    private static final int MASK = SIZE-1;

    private MemorySegment nativeSegment;
    private MemorySegment heapSegment;
    private ByteBuffer nativeBuffer;
    private ByteBuffer heapBuffer;
    private byte[] array;
    private int startsIdx = 0;
    private int[] starts;

    @Setup(Level.Trial) public void setup() {
        int base = (int) (Math.random()*1_000_000_000);
        array = new byte[SIZE];
        nativeBuffer = ByteBuffer.allocateDirect(SIZE);
        heapBuffer = ByteBuffer.allocate(SIZE);
        //noinspection resource
        nativeSegment = Arena.global().allocate(SIZE);
        heapSegment = MemorySegment.ofArray(new byte[SIZE]);
        byte v = (byte) base;
        for (int i = 0; i < SIZE; i++) {
            array[i] = v++;
            heapBuffer.put(i, v++);
            nativeBuffer.put(i, v++);
            heapSegment.set(JAVA_BYTE, i, v++);
            nativeSegment.set(JAVA_BYTE, i, v++);
        }
        starts = new int[SIZE];
        for (int i = 0; i < SIZE; i++) starts[i] = (int) (Math.random()*SIZE);
        try { Thread.sleep(50); } catch (InterruptedException ignored) {}
    }

    @Benchmark public int array() {
        int acc = 0, start = starts[startsIdx = (startsIdx+1) & MASK];
        for (int i = start, idx = 0, val; i < N_JUMPS; i++, idx = (idx+32*val)&MASK) {
            acc ^=       array[idx++];
            acc ^=       array[idx++];
            acc ^=       array[idx++];
            acc ^= val = array[idx++];
        }
        return acc;
    }

    @Benchmark public int heapBuffer() {
        int acc = 0, start = starts[startsIdx = (startsIdx+1) & MASK];
        for (int i = start, idx = 0, val; i < N_JUMPS; i++, idx = (idx+32*val)&MASK) {
            acc ^=       heapBuffer.get(idx);
            acc ^=       heapBuffer.get(idx);
            acc ^=       heapBuffer.get(idx);
            acc ^= val = heapBuffer.get(idx);
        }
        return acc;
    }


    @Benchmark public int nativeBuffer() {
        int acc = 0, start = starts[startsIdx = (startsIdx+1) & MASK];
        for (int i = start, idx = 0, val; i < N_JUMPS; i++, idx = (idx+32*val)&MASK) {
            acc ^=       nativeBuffer.get(idx);
            acc ^=       nativeBuffer.get(idx);
            acc ^=       nativeBuffer.get(idx);
            acc ^= val = nativeBuffer.get(idx);
        }
        return acc;
    }

    @Benchmark public int heapSegment() {
        int acc = 0, start = starts[startsIdx = (startsIdx+1) & MASK];
        for (int i = start, idx = 0, val; i < N_JUMPS; i++, idx = (idx+32*val)&MASK) {
            acc ^=       heapSegment.get(JAVA_BYTE, idx);
            acc ^=       heapSegment.get(JAVA_BYTE, idx);
            acc ^=       heapSegment.get(JAVA_BYTE, idx);
            acc ^= val = heapSegment.get(JAVA_BYTE, idx);
        }
        return acc;
    }

    @Benchmark public int nativeSegment() {
        int acc = 0, start = starts[startsIdx = (startsIdx+1) & MASK];
        for (int i = start, idx = 0, val; i < N_JUMPS; i++, idx = (idx+32*val)&MASK) {
            acc ^=       nativeSegment.get(JAVA_BYTE, idx);
            acc ^=       nativeSegment.get(JAVA_BYTE, idx);
            acc ^=       nativeSegment.get(JAVA_BYTE, idx);
            acc ^= val = nativeSegment.get(JAVA_BYTE, idx);
        }
        return acc;
    }
}
