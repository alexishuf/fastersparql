package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.mustcall.qual.MustCall;

import java.lang.foreign.MemorySegment;

@MustCall("close")
public interface NakedRopeFactory extends AutoCloseable {
    /** A {@link MemorySegment#ofArray(byte[])} for {@link #utf8()}. */
    MemorySegment segment();

    /** Byte array that holds the string. The array may be shared with other strings. */
    byte[] utf8();

    /** Index into {@link #utf8()}  where this string starts (first byte) */
    int begin();

    /** The number of UTF-8 bytes in the string. */
    int len();

    /**
     * Mark the data as taken.
     *
     * <p>Once this method returns:</p>
     * <ul>
     *     <li>The bytes between {@link #begin()} and {@link #begin()}{@code +}{@link #len()}
     *         Will become immutable and subject to collection by the GC once there are no more
     *         indirect references to {@link #utf8()} </li>
     *     <li>The {@link RopeFactory} or {@link PrivateRopeFactory} that provided this
     *     {@link NakedRopeFactory} instance will behave as if
     *     {@link RopeFactory#take()}/{@link PrivateRopeFactory#take()} had been called</li>
     * </ul>
     */
    @Override void close();
}
