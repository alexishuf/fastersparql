package com.github.alexishuf.fastersparql.client.model.batch;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A Batch is an array with elements in positions 0 (inclusive) to {@code size} (exclusive).
 * @param <T> the type of elements in the batch
 */
public final class Batch<T> {
    public T[] array;
    public @NonNegative int size;

    /* --- --- --- constructors & factories --- --- --- */

    public Batch(T[] array, int size) {
        this.array = array;
        this.size = size;
    }

    public Batch(Class<T> elementClass, int capacity) {
        //noinspection unchecked
        this.array = (T[])Array.newInstance(elementClass, capacity);
    }

    /* --- --- --- methods --- --- --- */

    public T[]     array() { return array; }
    public int      size() { return size; }
    public boolean empty() { return size == 0; }

    /** Add {@code item} to the end of this batch, growing the array if needed. */
    public void add(T item) {
        if (size >= array.length)
            array = Arrays.copyOf(array, size <= 10 ? 15 : size + (size>>1));
        array[size++] = item;
    }

    /** Add all items from {@code src[begin]} up to (and including) {@code src[begin+length-1]}. */
    public void add(T[] src, int begin, int length) {
        int required = size + length;
        if (array.length < required) {
            int capacity = Math.max(array.length, 10);
            while (capacity < required)
                capacity += capacity>>1;
            array = Arrays.copyOf(array, capacity);
        }
        System.arraycopy(src, begin, array, size, length);
        size += length;
    }

    /** Set {@code size} to zero. */
    public void clear() { size = 0; }

    /**
     * Add all elements of this batch to the given collection.
     *
     * @param collection the destination collection
     * @return the number of elements added to the collection: {@link Batch#size()}.
     */
    public int drainTo(Collection<? super T> collection) {
        if (collection instanceof ArrayList<?> al)
            al.ensureCapacity(al.size()+size);
        //noinspection ManualArrayToCollectionCopy
        for (int i = 0; i < size; i++)
            collection.add(array[i]);
        return size;
    }


    public @Nullable T reduce(BiFunction<T, T, T> combine) {
        if (size == 0) return null;
        T acc = array[0];
        for (int i = 1, n = size; i < n; i++)
            acc = combine.apply(acc, array[i]);
        return acc;
    }

    public <R> R reduce(R initial, BiFunction<R, T, R> combine) {
        R acc = initial;
        for (int i = 0, n = size; i < n; i++)
            acc = combine.apply(acc, array[i]);
        return acc;
    }

    public void forEach(Consumer<T> consumer) {
        for (int i = 0, n = size; i < n; i++)
            consumer.accept(array[i]);
    }

    /* --- --- --- java.lang.Object methods --- --- --- */

    @Override public String toString() {
        var b = new StringBuilder().append('[');
        for (int i = 0; i < size; i++)
            b.append(array[i]).append(", ");
        b.setLength(Math.max(1, b.length()-2));
        return b.append(']').toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Batch<?> rhs)) return false;
        boolean ok = size == rhs.size;
        for (int i = 0; ok && i < size; i++) ok = Objects.equals(array[i], rhs.array[i]);
        return ok;
    }

    @Override public int hashCode() {
        if (size <= 0) return 0;
        int result = 1;
        for (int i = 0; i < size; i++) {
            T e = array[i];
            result = 31*result + (e == null ? 0 : e.hashCode());
        }
        return result;
    }
}
