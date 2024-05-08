package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.RDF;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.XSD;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.CONSTANT;

public abstract sealed class PrefixMap extends AbstractOwned<PrefixMap> {
    public static final FinalSegmentRope XSD_NAME = asFinal("xsd");
    public static final FinalSegmentRope RDF_NAME = asFinal("rdf");

    /**
     * Given a {@code PREFIX name: <iri>} SPARQL fragment, {@code data[i] stores {@code name}
     * and {@code data[i + (data.length>>1)]} stores {@code <iri>}.
     */
    private final RopeArrayMap map = RopeArrayMap.create().takeOwnership(this);


    public static Orphan<PrefixMap> create() { return new Concrete(); }
    private PrefixMap() {}
    private static final class Concrete extends PrefixMap implements Orphan<PrefixMap> {
        @Override public PrefixMap takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable PrefixMap recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        map.recycle(this);
        return null;
    }

    /** Remove all prefix -> uri mappings from this {@link PrefixMap} */
    @SuppressWarnings("unused") public void clear() { map.clear(); }

    /** Get the number of prefix -> URI mappings in this {@link PrefixMap} */
    public int size() { return map.size(); }

    /** Removes all mappings and add mappings for {@code xsd} and {@code rdf}.  */
    public @This PrefixMap resetToBuiltin() {
        map.resetToCopy(BUILTIN);
        return this;
    }
    private static final RopeArrayMap BUILTIN;
    static {
        BUILTIN = RopeArrayMap.create().takeOwnership(CONSTANT);
        BUILTIN.put(XSD_NAME, XSD);
        BUILTIN.put(RDF_NAME, RDF);
    }

    /**
     * Equivalent to {@link #clear()} followed by {@link #addAll(PrefixMap)}, but more efficient
     * as there will be no rope comparisons, even if {@code other} has a size that triggers a
     * sorted {@link RopeArrayMap}.
     *
     * @param other the source mappings, will not be changed.
     * @return {@code this} containing all and only the mappings in {@code other}
     */
    @SuppressWarnings("UnusedReturnValue")
    public @This PrefixMap resetToCopy(PrefixMap other) {
        map.resetToCopy(other.map);
        return this;
    }

    @SuppressWarnings("unused")
    public Rope key(int i) { return map.key(i); }
    @SuppressWarnings("unused") public Rope value(int i) { return map.value(i); }

    /** Tests whether {@code name} is mapped to some IRI in this {@link PrefixMap}. */
    @SuppressWarnings("unused") public boolean contains(Rope name) { return map.get(name) != null; }

    /**
     * Maps {@code name} to {@code iri} so that {@code name:} expands to {@code iri}.
     *
     * @param name the prefix name (not including the trailing ':'
     * @param iri Preferably a {@link Term} instance. If this is not a {@link Term}, the
     *            surrounding angled brackets are optional, and it will be converted into a
     *            {@link Term} IRI. If this is a {@link Term}, {@link Term#type()} must be
     *            {@link Term.Type#IRI}, else an {@link IllegalArgumentException} will be thrown.
     */
    public void add(Rope name, Rope iri) {
        //sanitize iri
        if (iri instanceof Term t) {
            if (t.type() != Term.Type.IRI)
                throw new IllegalArgumentException("iri is a non-IRI Term");
        } else if (iri.len == 0) {
            iri = Term.EMPTY_IRI;
        } else {
            if (iri.get(0) != '<')
                throw new IllegalArgumentException("iri prefix does not start with <");
            int end = iri.len - (iri.get(iri.len-1) == '>' ? 1 : 0);
            var prefix = asFinal(iri, 0, end);
            iri = Term.wrap(prefix, Term.CLOSE_IRI);
        }
        map.put(asFinal(name), iri);
    }

    /** Analogous to {@link #add(Rope, Rope)} but will not copy {@code name} nor {@code iri}. */
    public void addRef(FinalSegmentRope name, Term iri) {
        if (iri.type() != Term.Type.IRI)
            throw new IllegalArgumentException("iri is a non-IRI Term");
        map.put(name, iri);
    }

    /** Add all prefixes in other to {@code this}, overwriting existing mappings. */
    public void addAll(PrefixMap other) { map.putAll(other.map); }

    /**
     * Given a previous {@code add("name", "<...#>"} call and {@code "name:local"} in
     * {@code str.sub(begin, localNameEnd)}, return an IRI {@link Term} for
     * {@code <...#local>}.
     *
     * <p>If {@code str.sub(begin, colonIdx)} was not a name previously given in an
     * {@link PrefixMap#add(Rope, Rope)} call, return {@code null}. Note that both
     * the prefix name and the local part may be empty.</p>
     *
     * @param str a Rope containing something akin to {@code name:local}
     * @param begin where a prefixed IRI reference starts in {@code str}
     * @param colonIdx index in {@code str} of the ':' splitting the prefix name from the
     *                 local name.
     * @param localNameEnd {@code str.len()} or index of the first byte after the local name
     *                     of the prefixed IRI reference to be expanded.
     * @return a {@link Term} or {@code null} if the prefix name was not previously
     *         {@link PrefixMap#add(Rope, Rope)}ed to this {@link PrefixMap}.
     */
    public @Nullable Term expand(Rope str, int begin, int colonIdx, int localNameEnd) {
        Term prefix = (Term) map.get(str, begin, colonIdx);
        int lnBegin = colonIdx + 1;
        if (prefix == null || localNameEnd == lnBegin) return prefix;

        var local = RopeFactory.make(localNameEnd-lnBegin+1)
                               .add(str, lnBegin, localNameEnd).add('>').take();
        return Term.wrap(prefix.shared(), local);
    }

    /**
     * Equivalent to {@link PrefixMap#expand(Rope, int, int, int)} but computes
     * {@code colonIdx}.
     */
    public @Nullable Term expand(Rope str, int begin, int localNameEnd) {
        int colon = str.skipUntil(begin, localNameEnd, ':');
        return colon == localNameEnd ? null : expand(str, begin, colon, localNameEnd);
    }

    /** Equivalent to {@code expandTerm(str, 0, str.len())}. */
    public @Nullable Term expand(Rope str) {
        int end = str.len(), colon = str.skipUntil(0, end, ':');
        return colon == end ? null : expand(str, 0, colon, end);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override public String toString() {
        return map.toString();
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof PrefixMap rhs && map.equals(rhs.map);
    }

    @Override public int hashCode() {
        return map.hashCode();
    }

    /* --- --- --- test helpers --- --- --- */

    void add(String name, String iri) { add(asFinal(name), asFinal(iri)); }

    @Nullable Term expand(String str) { return expand(asFinal(str)); }
    @Nullable Term expand(String str, int begin, int end) {
        return expand(asFinal(str), begin, end);
    }
    @Nullable Term expand(String str, int begin, int colonIdx, int end) {
        return expand(asFinal(str), begin, colonIdx, end);
    }
}
