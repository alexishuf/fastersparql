package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.IndexOrHigh;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Stream;

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
public sealed class Vars extends AbstractList<SegmentRope> implements RandomAccess, Set<SegmentRope> {
    public static final Vars EMPTY = new Vars(new SegmentRope[0], 0L, 0);

    protected SegmentRope[] array;
    protected long has;
    protected @IndexOrHigh("array") int size;

    /* --- --- --- constructor & factory methods --- --- --- */

    private Vars(SegmentRope[] array, long has, int size) {
        this.array = array;
        this.has = has;
        this.size = size;
    }

    /** Create a {@link Vars} wrapping (by reference) the varargs array */
    public static Vars of(SegmentRope... vars) {
        if (vars == null || vars.length == 0) return EMPTY;
        return new Vars(vars, hashAll(vars, vars.length), vars.length);
    }

    /** Convert the strings into {@link Rope}s and add them into a new {@link Vars} instance. */
    public static Vars of(CharSequence... strings) {
        if (strings == null || strings.length == 0) return EMPTY;
        Mutable set = new Mutable(strings.length);
        for (var s : strings)
            set.add(new ByteRope(s));
        return set;
    }

    /** Wrap an existing array (by reference) into a Vars instance */
    public static Vars wrapSet(SegmentRope[] array, @IndexOrHigh("array") int size) {
        return new Vars(array, hashAll(array, size), size);
    }

    /** Copy a non-distinct collection into a new mutable Vars instance. */
    public static Mutable from(Collection<?> collection) {
        return from(collection, collection.size());
    }

    /** Collect {@code stream} into a new mutable {@link Vars} set. */
    public static Mutable from(Stream<?> stream) {
        return from(stream.toList());
    }

    /** Copy a non-distinct collection into a new mutable Vars instance with given capacity. */
    public static Mutable from(Collection<?> collection, int capacity) {
        Mutable vars = new Mutable(new SegmentRope[capacity], 0L, 0);
        vars.addAllConverting(collection);
        return vars;
    }

    /** Copy all items from a distinct collection into a new mutable {@link Vars} instance */
    public static Mutable fromSet(Collection<?> set) {
        return fromSet(set, set.size());
    }

    /** Copy {@link Vars} instance  into a new mutable {@link Vars} with given capacity */
    public static Mutable fromSet(Vars set, int capacity) {
        int size = set.size;
        return new Mutable(Arrays.copyOf(set.array, Math.max(capacity, size)), set.has, size);
    }

    /** Copy a distinct collection into a new mutable {@link Vars} instance with given capacity */
    public static Mutable fromSet(Collection<?> set, int capacity) {
        if (set instanceof Vars vars) return Vars.fromSet(vars, capacity);
        int size = set.size();
        SegmentRope[] array = new SegmentRope[Math.max(size, capacity)];
        int i = 0;
        for (Object o : set)
            array[i++] = o instanceof SegmentRope r ? r : new ByteRope(o.toString());
        return new Mutable(array, hashAll(array, size), size);
    }

    /* --- --- --- factory methods --- --- --- */

    /** Get a {@link Vars} (which may be {@code this}) with all items
     *  in {@code this} and in {@code right}. */
    public final Vars union(Vars right) {
        if (containsAll(right)) return this;
        SegmentRope[] array = grownFor(right, 0);
        if (array == this.array) return this;
        var copy = new Mutable(array, has, size);
        copy.addAll(right);
        return copy.size == size ? this : copy;
    }

    /** Get a {@link Vars} (which may be {@code this}) with all items in {@code this} that are
     *  not present in {@code right} */
    public final Vars minus(Vars right) {
        int size = this.size, rightSize = right.size;
        if (size == 0 || rightSize == 0) return this;
        if (size < 32) {
            int remove = 0;
            long has = 0, rightHas = right.has;
            SegmentRope[] array = this.array, rArray = right.array;
            outer: for (int i = 0; i < size; i++) {
                SegmentRope name = array[i];
                long bit = 1L << name.hashCode();
                if ((rightHas & bit) == 0) {
                    has |= bit;
                } else {
                    for (int j = 0; j < rightSize; j++) {
                        if (rArray[j].equals(name)) { remove |= 1 << i; continue outer; }
                    }
                }
            }
            if (remove == 0) return this;
            SegmentRope[] rem = new SegmentRope[array.length];
            int nRem = 0;
            for (int i = 0; i < size; i++) {
                if ((remove & (1 << i)) == 0) rem[nRem++] = array[i];
            }
            return new Mutable(rem, has, nRem);
        } else {
            return coldMinus(right);
        }
    }

    private Vars coldMinus(Vars right) {
        SegmentRope[] array = this.array;
        Mutable rem = new Mutable(array.length);
        for (int i = 0, size = this.size; i < size; i++) {
            SegmentRope name = array[i];
            if (!right.contains(name)) rem.add(name);
        }
        return rem.size == size ? this : rem;
    }


    /** Get the subset of items in {@code this} that are also present in {@code other} */
    public final Vars intersection(Collection<SegmentRope> other) {
        SegmentRope[] array = new SegmentRope[Math.min(size, other.size())];
        long has = 0;
        int size = 0;
        for (Rope s : other) {
            long mask = 1L << s.hashCode();
            if ((this.has & mask) != 0) {
                for (int i = 0; i < this.size; i++) {
                    if (s.equals(this.array[i])) {
                        has |= mask;
                        array[size++] = SegmentRope.of(s);
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
     * Get a reference to the backing {@code SegmentRope[]}.
     *
     * <p>Changes to the array will reflect on {@code this} {@link Vars} even if the instance
     * is immutable. Likewise, mutations on the {@link Vars} object may cause this method
     * to return a different reference.</p>
     */
    public final SegmentRope[] array() { return array; }


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
    @EnsuresNonNullIf(expression = "#1", result = true)
    public boolean intersects(Collection<? extends Rope> other) {
        if (other == null || other instanceof Vars v && (has & v.has) == 0) return false;
        for (var s : other) {
            if (indexOf(s) > -1) return true;
        }
        return false;
    }

    /* --- --- --- overridden query methods --- --- --- */

    @Override public final SegmentRope get(int index) {
        if (index >= size) throw new IndexOutOfBoundsException(index);
        return array[index];
    }

    @Override public final int lastIndexOf(Object o) { return indexOf(o); }

    /**
     * Gets the {@code i} such that
     * <pre>
     *     SegmentRope name = varOrVarName instanceof Term t && t.type() == VAR ? t.name() : varOrVarName
     *     get(i).equals(name)
     * </pre>
     *
     * @param varOrVarName var name or var (as a {@link Term}) to search for
     * @return index of the var in this {@link Vars} or -1 if it is not present.
     */
    @Override public final int indexOf(Object varOrVarName) {
        if (varOrVarName instanceof Term t) varOrVarName = t.name();
        if (varOrVarName == null) return -1;
        if ((has & (1L << varOrVarName.hashCode())) == 0) return -1;
        for (int i = 0; i < size; i++) {
            if (varOrVarName.equals(array[i])) return i;
        }
        return -1;
    }

    /** Equivalent to {@code indexOf(varOrVarName) >= 0}. */
    @Override public boolean contains(Object varOrVarName) { return indexOf(varOrVarName) >= 0; }

    @Override public final int size() { return size; }

    @Override public final boolean isEmpty() { return size == 0; }

    @Override public final Spliterator<SegmentRope> spliterator() { return super.spliterator(); }

    @Override public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Collection<?> coll) || size != coll.size()) return false;
        if (o instanceof Vars v && has != v.has) return false;
        int i = 0;
        for (Object item : coll) {
            if (!array[i++].equals(item)) return false;
        }
        return true;
    }

    /* --- --- --- specialized mutation methods --- --- --- */

    public boolean add(Term var) { throw new UnsupportedOperationException(); }
    public boolean addAll(@NonNull Vars other) {
        throw new UnsupportedOperationException();
    }

    /* --- --- --- mutable subclass --- --- --- */

    /** A Mutable {@link Vars} instance */
    public final static class Mutable extends Vars {
        private Mutable(SegmentRope[] array, long has, int size) { super(array, has, size); }

        /** Create an empty mutable {@link Vars} backed by an array of length {@code capacity}. */
        public Mutable(int capacity) { super(new SegmentRope[capacity], 0L, 0); }

        /**
         * Adds the given var name (will call {@link Term#name()} if given a {@link Term.Type#VAR})
         *
         * @param name element whose presence in this collection is to be ensured
         * @return {@code true} iff the var was not already present.
         */
        @Override public boolean add(SegmentRope name) {
            if (name == null) throw new NullPointerException();
            if (indexOf(name) >= 0) return false;
            if (size >= array.length) //must grow array
                array = grownFor(null, 0); // do not inline: cold code
            array[size++] = name;
            has |= 1L << name.hashCode();
            return true;
        }

        @Override public boolean add(Term var) {
            if (!var.isVar())
                throw new IllegalArgumentException("Non-var Term instance");
            return add(new ByteRope(var.toArray(1, var.len)));
        }

        @Override public boolean addAll(@NonNull Collection<? extends SegmentRope > c) {
            return addAllConverting(c);
        }

        @Override public boolean addAll(@NonNull Vars other) {
            int size = this.size;
            outer: for (int i = 0, otherSize = other.size; i < otherSize; i++) {
                SegmentRope name = other.get(i);
                long bit = 1L << name.hashCode();
                if ((has & bit) != 0) {
                    for (int j = 0; j < size; j++) {
                        if (name.equals(array[j])) continue outer;
                    }
                }
                has |= bit;
                if (this.size == array.length)
                    grownFor(other, i);
                array[this.size++] = name;
            }
            return this.size != size;
        }

        private boolean addAllConverting(@NonNull Collection<?> c) {
            int oldSize = size;
            int i = 0;
            outer: for (Object object : c) {
                if (object == null) continue;
                SegmentRope s;
                if (object instanceof Term t) {
                    s = t.name();
                    if (s == null) throw new IllegalArgumentException("Non-var Term");
                } else if (object instanceof SegmentRope r) {
                    s = r;
                } else {
                    s = new ByteRope(object.toString());
                }
                long mask = 1L << s.hashCode();
                if ((has & mask) != 0) { // s may be present
                    for (int j = 0; j < size; j++) { // continue outer if s is in array
                        if (s.equals(array[j])) continue outer;
                    }
                }
                if (size == array.length) // we must grow array
                     array = grownFor(c, i); // do a single reallocation. Do not inline as this is cold code
                array[size++] = s;
                has |= mask;
                ++i;
            }
            return size != oldSize;
        }

        @Override public SegmentRope remove(int index) {
            if (index >= size)
                throw new IndexOutOfBoundsException(index);
            SegmentRope old = array[index];
            arraycopy(array, index+1, array, index, size-(index+1));
            --size;
            has = hashAll(array, size);
            return old;
        }

        @Override public void clear() { has = size = 0; }
    }

    /* --- --- --- helpers --- --- --- */

    private static long hashAll(SegmentRope[] array, int size) {
        long has = 0L;
        for (int i = 0; i < size; i++)
            has |= 1L << array[i].hashCode();
        return has;
    }

    protected final SegmentRope[] grownFor(@Nullable Collection<?> source, int from) {
        int next = Math.max(10, array.length + (array.length>>1));
        // when this is called with non-null source, it typically overlaps (join vars) or
        // is equal to this (union/gather) as cartesian products are not frequent.
        // If a 50% growth cannot handle the worst case scenario of no intersection,
        // then we count the precise growth need for adding source and resize to accommodate that count
        if (source != null && size+(source.size()-from) > next) {
            if (source instanceof List<?> list)
                next = size + novelItems(list, MAX_VALUE, from);
            else
                next = size + novelItems(source, MAX_VALUE);
            if (next == size) return array;
        }
        return Arrays.copyOf(array, next);
    }
}
