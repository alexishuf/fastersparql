package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.IndexOrHigh;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

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
    public static final int BYTES = 16 + 2*4 + 8;
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
            set.add(FinalSegmentRope.asFinal(s));
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
            array[i++] = FinalSegmentRope.asFinal(o);
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
        return copy;
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
                if ((rightHas & bit) != 0) {
                    for (int j = 0; j < rightSize; j++) {
                        if (rArray[j].equals(name)) { remove |= 1 << i; continue outer; }
                    }
                }
                has |= bit;
            }
            if (remove == 0) return this;
            if (remove == (1L << size) - 1) return EMPTY;
            SegmentRope[] survivors = new SegmentRope[array.length];
            int nSurvivors = 0;
            for (int i = 0; i < size; i++) {
                if ((remove & (1 << i)) == 0) survivors[nSurvivors++] = array[i];
            }
            return new Mutable(survivors, has, nSurvivors);
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
    public final Vars intersection(Collection<? extends SegmentRope> other) {
        SegmentRope[] array = new SegmentRope[Math.min(size, other.size())];
        long has = 0;
        int size = 0;
        for (Rope s : other) {
            long mask = 1L << s.hashCode();
            if ((this.has & mask) != 0) {
                for (int i = 0; i < this.size; i++) {
                    if (s.equals(this.array[i])) {
                        has |= mask;
                        array[size++] = FinalSegmentRope.asFinal(s);
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

    /** Test whether {@code this} and {@code other} share at least one item. */
    @EnsuresNonNullIf(expression = "#1", result = true)
    public boolean intersects(Collection<? extends Rope> other) {
        if (other == null || other instanceof Vars v && (has & v.has) == 0) return false;
        for (var s : other) {
            if (indexOf(s) > -1) return true;
        }
        return false;
    }

    /** Test whether {@code this} and {@code other} share at least one item. */
    @EnsuresNonNullIf(expression = "#1", result = true)
    public boolean intersects(Vars other) {
        if (other == null || (has & other.has) == 0) return false;
        SegmentRope[] array = this.array, oArray = other.array;
        for (int i = 0, size = this.size, oSize = other.size; i < size; i++) {
            var name = array[i];
            for (int j = 0; j < oSize; j++) {
                if (name.equals(oArray[j])) return true;
            }
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
     * Gets the {@code i} such that {@code get(i).equals(obj)}.
     *
     * @param obj var obj to search for
     * @return index of the var in this {@link Vars} or -1 if it is not present.
     */
    @Override public final int indexOf(Object obj) {
        if (obj == null)           return -1;
        if (obj instanceof Term t) return indexOf(t);
        var name = FinalSegmentRope.asFinal(obj);
        if ((has & (1L << name.hashCode())) == 0) return -1;
        for (int i = 0; i < size; i++) {
            if (name.equals(array[i])) return i;
        }
        return -1;
    }

    /**
     * Gets the {@code i} such that {@code get(i).equals(name)}.
     *
     * @param name var name to search for
     * @return index of the var in this {@link Vars} or -1 if it is not present.
     */
    public final int indexOf(SegmentRope name) {
        if (name == null) return -1;
        if ((has & (1L << name.hashCode())) == 0) return -1;
        for (int i = 0; i < size; i++) {
            if (name.equals(array[i])) return i;
        }
        return -1;
    }

    /**
     * Get the {@code i} such that {@code get(i).equals(term.name())} or -1
     *
     * @param term a {@link Term} that MAY be {@link Term.Type#VAR}. if ground, will return -1.
     * @return -1 if {@link Term#name()} is not present in this set, if {@code term} is
     *         null or if {@code term} is not a var. Else return the {@code i} such that
     *         {@code get(i).equals(term.name()}.
     */
    public final int indexOf(Term term) {
        if (term == null || !term.isVar()) return -1;
        for (int i = 0, n = size; i < n; i++) {
            SegmentRope local = term.local(), name = array[i];
            if (name.len+1 == local.len && name.has(0, local, 1, local.len))
                return i;
        }
        return -1;
    }

    /** Equivalent to {@code indexOf(varOrVarName) >= 0}. */
    @Override public boolean contains(Object varOrVarName) { return indexOf(varOrVarName) >= 0; }
    public           boolean contains(SegmentRope name)    { return indexOf(name)         >= 0; }
    public           boolean contains(Term term)           { return indexOf(term)         >= 0; }

    public boolean containsAll(Vars vars) {
        if ((vars.has &~ has) != 0) return false;
        SegmentRope[] array = this.array, other = vars.array;
        int size = this.size, otherSize = vars.size;
        for (int i = 0; i < otherSize; i++) {
            SegmentRope name = other[i];
            boolean found = false;
            for (int j = 0; j < size && !found; j++)
                found = name.equals(array[j]);
            if (!found)
                return false;
        }
        return true;
    }

    @Override public final int size() { return size; }

    @Override public final boolean isEmpty() { return size == 0; }

    @Override public final @NonNull Iterator<SegmentRope> iterator() { return super.iterator(); }

    @Override public final Spliterator<SegmentRope> spliterator() { return super.spliterator(); }

    public boolean equals(Vars o) {
        if (o == this) return true;
        if (o == null || has != o.has) return false;
        int size = this.size;
        if (size != o.size) return false;
        SegmentRope[] array = this.array, oArray = o.array;
        for (int i = 0; i < size; i++) {
            if (!array[i].equals(oArray[i])) return false;
        }
        return true;
    }

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
    public @Nullable SegmentRope set(int i, Term var) { throw new UnsupportedOperationException(); }
    @SuppressWarnings("UnusedReturnValue") public boolean addAll(@NonNull Vars other) {
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
            if (name == null)
                throw new NullPointerException();
            return indexOf(name) < 0 && add0(name);
        }

        private boolean add0(SegmentRope name) {
            if (size >= array.length) //must grow array
                array = grownFor(Collections.emptyList(), 0);
            array[size++] = name;
            has |= 1L << name.hashCode();
            return true;
        }

        @Override public boolean add(Term var) {
            if (!var.isVar())
                throw new IllegalArgumentException("Non-var Term instance");
            return indexOf(var) < 0 && add0(FinalSegmentRope.asFinal(var, 1, var.len));
        }

        @Override public SegmentRope set(int index, SegmentRope name) {
            if (name == null) throw new NullPointerException();
            if (index >= size || index < 0) throw new IndexOutOfBoundsException(index);
            var old = array[index];
            int currentIdx = indexOf(name);
            if (currentIdx < 0)  {
                array[index] = name;
                has = (has & ~(1L << old.hashCode())) | (1L << name.hashCode());
            } else if (currentIdx != index) {
                throw new IllegalArgumentException("name "+name+" aready set at index "+currentIdx);
            }
            return old;
        }


        @Override public @Nullable SegmentRope set(int i, Term var) {
            if (!var.isVar())
                throw new IllegalArgumentException("Non-var Term instance");
            return set(i, FinalSegmentRope.asFinal(var, 1, var.len));
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
                    array = grownFor(other, i);
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
                } else {
                    s = FinalSegmentRope.asFinal(object);
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

    protected final SegmentRope[] grownFor(Collection<?> source, int from) {
        int items = source.size() - from;
        int n = Math.max(10, Math.max(size+items, array.length + (array.length>>1)));
        return Arrays.copyOf(array, n);
    }
}
