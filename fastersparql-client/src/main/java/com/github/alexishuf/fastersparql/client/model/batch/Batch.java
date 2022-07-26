package com.github.alexishuf.fastersparql.client.model.batch;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A Batch is an array with elements in positions 0 (inclusive) to {@code size} (exclusive).
 * @param <T> the type of elements in the batch
 */
public sealed class Batch<T> {
    public T[] array;
    public int size;

    /* --- --- --- constructors & factories --- --- --- */

    public Batch(T[] array, int size) {
        this.array = array;
        this.size = size;
    }

    public Batch(Class<T> elementClass, int min, int max) {
        //noinspection unchecked
        this.array = (T[]) Array.newInstance(elementClass, preferredSize(min, max));
    }

    public Batch(Class<T> elementClass) {
        //noinspection unchecked
        this.array = (T[])Array.newInstance(elementClass, 0);
    }

    public static int preferredSize(int min, int max) {
        if      (max <= 2*min || max <= min+32) return max;
        else if (min < 64)                      return Math.min(max, 64);
        else                                    return min;
    }

    /* --- --- --- builder --- --- --- */

    public static final class Builder<T> extends Batch<T> {
        private int min = 1, max = 65_536;
        private long minTs = 0;

        public Builder(BatchIt<T> it) {
            super(it.elementClass(), it.minBatch(), it.maxBatch());
            update(it);
        }

        public boolean ready() { return size >= max || (size >= min && System.nanoTime() >= minTs); }

        public Builder<T> update(BatchIt<?> it) {
            min = it.minBatch();
            max = it.maxBatch();
            minTs = System.nanoTime() + it.minWait(TimeUnit.NANOSECONDS);
            return this;
        }
    }

    /* --- --- --- constructors & factories --- --- --- */

    public final T[] array() { return array; }
    public final int size() { return size; }

    /** Add {@code item} to the end of this batch, growing the array if needed. */
    public final void add(T item) {
        if (size >= array.length)
            array = Arrays.copyOf(array, size + Math.min(8, size/2));
        array[size++] = item;
    }

    /** Set {@code size} to zero. */
    public final void clear() { size = 0; }

    /** Whether {@code size} is smaller than the array capacity. */
    public final boolean needsTrimming() { return size < array.length; }

    /** Get either {@code this} or a copy that has {@code array.length == size}. */
    public final Batch<T> trimmed() {
        return size < array.length ? new Batch<>(Arrays.copyOf(array, size), size) : this;
    }

    /**  Get a new {@link Batch} that is a copy of this but with {@code array.length == size}. */
    public final Batch<T> trimmedCopy() {
        return new Batch<>(Arrays.copyOf(array, size), size);
    }

    /** Get either {@code array} or a copy such that {@code copy.length == size}. */
    public final T[] trimmedArray() {
        return size < array.length ? Arrays.copyOf(array, size) : array;
    }

    /** Get a new copy of {@code array} with {@code copy.length == size}. */
    public final T[] trimmedArrayCopy() { return Arrays.copyOf(array, size); }

    /**
     * Add all elements of this batch to the given collection.
     *
     * @param collection the destination collection
     * @return the number of elements added to the collection: {@link Batch#size()}.
     */
    public final int drainTo(Collection<? super T> collection) {
        if (collection instanceof ArrayList<?> al)
            al.ensureCapacity(al.size()+size);
        //noinspection ManualArrayToCollectionCopy
        for (int i = 0; i < size; i++)
            collection.add(array[i]);
        return size;
    }

    @Override public final String toString() {
        var b = new StringBuilder().append('[');
        for (int i = 0; i < size; i++)
            b.append(array[i]).append(", ");
        b.setLength(Math.max(1, b.length()-2));
        return b.toString();
    }

    @Override public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Batch<?> rhs) || size != rhs.size) return false;
        for (int i = 0; i < size; i++) {
            if (!Objects.equals(array[i], rhs.array[i])) return false;
        }
        return true;
    }

    @Override public final int hashCode() {
        if (size <= 0) return 0;
        int result = 1;
        for (int i = 0; i < size; i++) {
            T e = array[i];
            result = 31*result + (e == null ? 0 : e.hashCode());
        }
        return result;
    }
}
