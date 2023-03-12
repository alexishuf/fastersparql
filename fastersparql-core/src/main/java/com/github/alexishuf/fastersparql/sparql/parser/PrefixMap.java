package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeWrapper;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.RDF;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.XSD;

public final class PrefixMap {
    public static final ByteRope XSD_NAME = new ByteRope("xsd");
    public static final ByteRope RDF_NAME = new ByteRope("rdf");

    /**
     * Given a {@code PREFIX name: <iri>} SPARQL fragment, {@code data[i] stores {@code name}
     * and {@code data[i + (data.length>>1)]} stores {@code <iri>}.
     */
    private final RopeArrayMap map = new RopeArrayMap();


    /** Remove all prefix -> uri mappings from this {@link PrefixMap} */
    public void clear() { map.clear(); }

    /** Get the number of prefix -> URI mappings in this {@link PrefixMap} */
    public int size() { return map.size(); }

    /** Removes all mappings and add mappings for {@code xsd} and {@code rdf}.  */
    public PrefixMap resetToBuiltin() {
        map.clear();
        map.put(XSD_NAME, XSD);
        map.put(RDF_NAME, RDF);
        return this;
    }

    @SuppressWarnings("unused")
    public Rope key(int i) { return map.key(i); }
    public Rope value(int i) { return map.value(i); }

    /** Tests whether {@code name} is mapped to some IRI in this {@link PrefixMap}. */
    public boolean contains(Rope name) { return map.get(name) != null; }

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
        } else {
            iri = Term.valueOf(RopeWrapper.forIri(iri).toRope(iri));
        }
        map.put(new ByteRope(name), iri);
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
        if (prefix == null || localNameEnd == colonIdx+1) return prefix;

        int id = prefix.flaggedDictId;
        if (id == 0)
            return expandNoId(prefix, str,  colonIdx, localNameEnd);
        return Term.prefixed(id, str, colonIdx+1, localNameEnd);
    }

    private Term expandNoId(Term prefix, Rope str, int colonIdx, int localNameEnd) {
        int prefixEnd = prefix.len() - 1, localOff = colonIdx + 1;
        return Term.wrap(new ByteRope(prefixEnd + localNameEnd - localOff + 1)
                .append(prefix, 0, prefixEnd)
                .append(str, localOff, localNameEnd)
                .append('>'));
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

    /* --- --- --- test helpers --- --- --- */

    void add(String name, String iri) { add(new ByteRope(name), new ByteRope(iri)); }

    @Nullable Term expand(String str) { return expand(new ByteRope(str)); }
    @Nullable Term expand(String str, int begin, int end) { return expand(new ByteRope(str), begin, end); }
    @Nullable Term expand(String str, int begin, int colonIdx, int end) { return expand(new ByteRope(str), begin, colonIdx, end); }
}
