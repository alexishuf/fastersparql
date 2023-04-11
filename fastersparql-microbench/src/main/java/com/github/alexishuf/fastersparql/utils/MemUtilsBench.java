package com.github.alexishuf.fastersparql.utils;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 0, jvmArgsAppend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class MemUtilsBench {
    @Param({"9", "32", "1024"})
    private int width;
    private byte[][] arrays;

    @Setup public void setup() {
        byte b = (byte)(Math.random()*128);
        arrays = new byte[128][];
        for (int row = 0; row < arrays.length; row++) {
            arrays[row] = new byte[width];
            for (int col = 0; col < arrays[row].length; col++)
                arrays[row][col] = b++;
        }
    }

    @Benchmark public byte[][] baseline() {
        for (int i = 0; i < arrays.length; i++) {
            byte[] a = arrays[i];
            Arrays.fill(a, i&3, a.length-(i&3), (byte)0);
        }
        return arrays;
    }

    @Benchmark public byte[][] loop() {
        for (int i = 0; i < arrays.length; i++) {
            byte[] a = arrays[i];
            for (int j = i&3, e = a.length-(i&3); j < e; j++) a[j] = 0;
        }
        return arrays;
    }

//    @Benchmark public byte[][] memUtils() {
//        for (int i = 0; i < arrays.length; i++) {
//            byte[] a = arrays[i];
//            MemUtils.clear(a, i&3, a.length-(i&3));
//        }
//        return arrays;
//    }
}
