package com.github.alexishuf.fastersparql;

import org.openjdk.jmh.annotations.*;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 2, time = 1)
@Fork(value = 1, warmups = 0, jvmArgsAppend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class Bench {

    private ByteBuffer buffer;
    private MemorySegment segment = null;
    private int idx;

    @Setup public void setup() {
        buffer = ByteBuffer.allocateDirect(128);
        for (int i = 0; i < 128; i++)
            buffer.put((byte)('0' + (i%10)));
        buffer.position(0).limit(128);
        segment = MemorySegment.ofBuffer(buffer);
        idx  = (int) (Math.random()*128);
    }

    @Benchmark public byte baseline() {
        return buffer.get(idx);
    }

    @Benchmark public byte cachedSegment() {
        MemorySegment segment = this.segment;
        if (segment == null)
            this.segment = segment = MemorySegment.ofBuffer(buffer);
        return segment.get(ValueLayout.JAVA_BYTE, idx);
    }

    @Benchmark public byte segment() {
        return MemorySegment.ofBuffer(buffer).get(ValueLayout.JAVA_BYTE, idx);
    }
}
