package com.github.alexishuf.fastersparql.client.model;

import org.checkerframework.checker.index.qual.IndexOrHigh;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.System.arraycopy;

/**
 * A small random-access, sequenced {@link Set} of non-null Strings.
 *
 * <p>This is more memory efficient than standard {@link Set} implementations because it does not
 * instantiate buckets nor nodes. Iteration is faster than a set as it consists of iterating
 * an array.</p>
 *
 * <p>This is also faster than a List due to a probabilistic implementation of
 * {@link HashSet#contains(Object)} implemented at the cost of a long that simulates 64
 * buckets. probabilistic here means <strong>certain</strong> non-membership and
 * <strong>probable</strong> membership. The speed advantage for {@link List#indexOf(Object)}
 * and {@link List#contains(Object)} only holds for small sets</p>
 *
 *
 * */
public sealed class Vars extends AbstractList<String> implements RandomAccess, Set<String> {
    public static final Vars EMPTY = new Vars(new String[0], 0L, 0);

    protected String[] array;
    protected long has;
    protected @IndexOrHigh("array") int size;

    /* --- --- --- constructor & factory methods --- --- --- */

    private Vars(String[] array, long has, int size) {
        this.array = array;
        this.has = has;
        this.size = size;
    }

    /** Create a {@link Vars} wrapping (by reference) the varargs array */
    public static Vars of(String... vars) {
        if (vars == null || vars.length == 0) return EMPTY;
        return new Vars(vars, hashAll(vars, vars.length), vars.length);
    }

    /** Wrap an existing array (by reference) into a Vars instance */
    public static Vars wrapSet(String[] array, @IndexOrHigh("array") int size) {
        return new Vars(array, hashAll(array, size), size);
    }

    /** Copy a non-distinct collection into a new mutable Vars instance. */
    public static Mutable from(Collection<String> collection) {
        return from(collection, collection.size());
    }

    /** Copy a non-distinct collection into a new mutable Vars instance with given capacity. */
    public static Mutable from(Collection<String> collection, int capacity) {
        Mutable vars = new Mutable(new String[capacity], 0L, 0);
        vars.addAll(collection);
        return vars;
    }

    /** Copy all items from a distinct collection into a new mutable {@link Vars} instance */
    public static Mutable fromSet(Collection<String> set) {
        return fromSet(set, set.size());
    }

    /** Copy a distinct collection into a new mutable {@link Vars} instance with given capacity */
    public static Mutable fromSet(Collection<String> set, int capacity) {
        int size = set.size();
        capacity = Math.max(size, capacity);
        String[] array;
        long has;
        if (set instanceof Vars v) {
            array = Arrays.copyOf(v.array, capacity);
            has = v.has;
        } else {
            has = hashAll((array = set.toArray(new String[capacity])), size);
        }
        return new Mutable(array, has, size);
    }

    /* --- --- --- factory methods --- --- --- */

    /** Get a {@link Vars} (which may be {@code this}) with all items
     *  in {@code this} and in {@code right}. */
    public final Vars union(Vars right) {
        String[] array = grownFor(right, 0);
        if (array == this.array) return this;
        var copy = new Mutable(array, has, size);
        copy.addAll(right);
        return copy.size == size ? this : copy;
    }

    /** Get a {@link Vars} (which may be {@code this}) with all items in {@code this} that are
     *  not present in {@code right} */
    public final Vars minus(Vars right) {
        String[] array = new String[size];
        long has = 0L;
        int size = 0;
        outer: for (int i = 0; i < this.size; i++) {
            String s = this.array[i];
            long mask = 1L << (s.hashCode()&63);
            if ((right.has & mask) != 0) {
                for (int j = 0; j < right.size; j++) {
                    if (s.equals(right.array[j])) continue outer;
                }
            }
            array[size++] = s;
            has |= mask;
        }
        return size == this.size ? this : new Mutable(array, has, size);
    }

    /** Get the subset of items in {@code this} that are also present in {@code other} */
    public final Vars intersection(Collection<String> other) {
        String[] array = new String[Math.min(size, other.size())];
        long has = 0;
        int size = 0;
        for (String s : other) {
            long mask = 1L << (s.hashCode() & 63);
            if ((this.has & mask) != 0) {
                for (int i = 0; i < this.size; i++) {
                    if (s.equals(this.array[i])) {
                        has |= mask;
                        array[size++] = s;
                        break;
                    }
                }
            }
        }
        if      (size == 0)                                       return EMPTY;
        else if (size == this.size)                               return this;
        else if (size == other.size() && other instanceof Vars v) return v;
        else                                                      return new Vars(array, has, size);
    }

    /* --- --- --- non-overridden query methods --- --- --- */

    /**
     * Get a reference to the backing {@code String[]}.
     *
     * <p>Changes to the array will reflect on {@code this} {@link Vars} even if the instance
     * is immutable. Likewise, mutations on the {@link Vars} object may cause this method
     * to return a different reference.</p>
     */
    public final String[] array() { return array; }


    /** Count up to {@code maxCount} items from {@code other} (starting at {@code from})
     *  which are not present in {@code this}. */
    public final int novelItems(List<?> list, int maxCount, int from) {
        int novel = 0;
        for (int i = from, size = list.size(); i < size; i++) {
            if (indexOf(list.get(i)) == -1 && ++novel >= maxCount) break;
        }
        return novel;
    }

    /** Count up to {@code maxCount} items from {@code other} not present in {@code this}. */
    public final int novelItems(Iterable<?> other, int maxCount) {
        int novel = 0;
        for (Object o : other) {
            if (indexOf(o) == -1 && ++novel >= maxCount) break;
        }
        return novel;
    }

    /** Test whether {@code this} and {@code other} share at least one item. */
    public boolean intersects(Collection<String> other) {
        if (other instanceof Vars v && (has & v.has) == 0) return false;
        for (String s : other) {
            if (indexOf(s) > -1) return true;
        }
        return false;
    }

    /* --- --- --- overridden query methods --- --- --- */

    @Override public final String get(int index) {
        if (index >= size) throw new IndexOutOfBoundsException(index);
        return array[index];
    }

    @Override public final int lastIndexOf(Object o) { return indexOf(o); }

    /** Equivalent to {@code indexOf(string.substring(begin, end))}. */
    public final int indexOf(String string, int begin, int end) {
        if (string == null || end <= begin) return -1;
        int len = end - begin;
        for (int i = 0; i < size; i++) {
            String candidate = array[i];
            if (candidate.length() == len && candidate.regionMatches(0, string, begin, len))
                return i;
        }
        return -1;
    }

    @Override public final int indexOf(Object o) {
        if (o == null) return -1;
        if ((has & (1L << (o.hashCode() & 63))) == 0) return -1;
        for (int i = 0; i < size; i++) {
            if (o.equals(array[i])) return i;
        }
        return -1;
    }

    @Override public boolean contains(Object o) { return indexOf(o) >= 0; }

    @Override public final int size() { return size; }

    @Override public final boolean isEmpty() { return size == 0; }

    @Override public final Spliterator<String> spliterator() { return super.spliterator(); }

    /* --- --- --- mutable subclass --- --- --- */

    /** A Mutable {@link Vars} instance */
    public final static class Mutable extends Vars {
        private Mutable(String[] array, long has, int size) { super(array, has, size); }

        /** Create an empty mutable {@link Vars} backed by an array of length {@code capacity}. */
        public Mutable(int capacity) { super(new String[capacity], 0L, 0); }

        @Override public boolean add(String s) {
            if (s == null)
                throw new NullPointerException();
            if (indexOf(s) >= 0)
                return false;
            if (size >= array.length) //must grow array
                array = grownFor(null, 0); // do not inline: cold code
            array[size++] = s;
            has |= 1L << (s.hashCode() & 63);
            return true;
        }

        @Override public boolean addAll(@NonNull Collection<? extends String> c) {
            int oldSize = size;
            int i = 0;
            outer: for (String s : c) {
                if (s == null) continue;
                long mask = 1L << (s.hashCode() & 63);
                if ((has & mask) != 0) { // s may be present
                    for (int j = 0; j < size; j++) { // continue outer if s is in array
                        if (s.equals(array[j])) continue outer;
                    }
                }
                if (size == array.length) // we must grow array
                     array = grownFor(c, i); // do a single reallocation. Do not inline as this is cold code
                array[size++] = s;
                has |= mask;
            }
            return size != oldSize;
        }

        @Override public String remove(int index) {
            if (index >= size)
                throw new IndexOutOfBoundsException(index);
            String old = array[index];
            arraycopy(array, index+1, array, index, size-(index+1));
            --size;
            has = hashAll(array, size);
            return old;
        }

        @Override public void clear() { has = size = 0; }
    }

    /* --- --- --- helpers --- --- --- */

    private static long hashAll(String[] array, int size) {
        long has = 0L;
        for (int i = 0; i < size; i++)
            has |= 1L << (array[i].hashCode()&63);
        return has;
    }

    protected final String[] grownFor(@Nullable Collection<?> source, int from) {
        int next = Math.max(10, array.length + (array.length>>1));
        // when this is called with non-null source, it typically overlaps (join vars) or
        // is equal to this (union/gather) as cartesian products are not frequent.
        // If a 50% growth cannot handle the worst case scenario of no intersection,
        // then we count the precise growth need for adding source and resize to accommodate that count
        if (source != null && size+source.size() > next) {
            if (source instanceof List<?> list)
                next = size + novelItems(list, MAX_VALUE, from);
            else
                next = size + novelItems(source, MAX_VALUE);
            if (next == size) return array;
        }
        return Arrays.copyOf(array, next);
    }
}
