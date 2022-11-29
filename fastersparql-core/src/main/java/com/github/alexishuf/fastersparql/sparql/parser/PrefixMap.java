package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.sparql.RDF;
import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public final class PrefixMap {
    private static final PrefixMap BUILTIN = new PrefixMap().resetToBuiltin();

    private String[] data = new String[10];
    private int prefixes;

    public static PrefixMap builtin() {
        if (BUILTIN.prefixes != 2)
            BUILTIN.resetToBuiltin();
        return BUILTIN;
    }

    /** Remove all prefix -> uri mappings from this {@link PrefixMap} */
    public void clear() {
        prefixes = 0;
    }

    /** Get the number of prefix -> URI mappings in this {@link PrefixMap} */
    public int size() { return prefixes; }

    /** Removes all mappings and add mappings for {@code xsd} and {@code rdf}.  */
    public PrefixMap resetToBuiltin() {
        prefixes = 2;
        data[0] = "xsd";
        data[1] = "rdf";
        data[5] = RDFTypes.XSD;
        data[6] = RDF.NS;
        return this;
    }

    /** Maps {@code prefix} to {@code uri} so that {@code prefix:} expands to {@code uri}. */
    public void add(String prefix, String uri) {
        int i = findPrefix(prefix);
        if (i >= 0) {
            data[i + (data.length>>1)] = uri;
        } else {
            if (prefixes<<1 >= data.length) {
                int mid = data.length >> 1;
                data = Arrays.copyOf(data, data.length<<1);
                System.arraycopy(data, mid, data, data.length>>1, mid);
            }
            data[prefixes] = prefix;
            data[prefixes + (data.length>>1)] = uri;
            ++prefixes;
        }
    }

    /**
     * Add all prefixes in other to {@code this}, overwriting existing mappings.
     * @param other source of new mappings.
     */
    public void addAll(PrefixMap other) {
        int prefixes = other.prefixes, mid = other.data.length>>1;
        String[] oData = other.data;
        for (int i = 0; i < prefixes; i++)
            add(oData[i], oData[mid+i]);
    }

    /**
     * Given {@code str.substring(start, localNameEnd).equals("prefix:localName")}, returns
     * that substring with {@code prefix:} replaced with the uri to which {@code prefix} maps to
     * in this {@link PrefixMap}.
     *
     * @param str a {@link String} containing a {@code prefix:localName} substring
     * @param start where {@code prefix:localName} starts within {@code str}
     * @param colonIdx index of the {@code :} in {@code str}
     * @param localNameEnd index of the first char after {@code prefix:localName} in {@code str}
     *                     (can be @code str.length()}).
     *
     * @return {@code uri("prefix")+"localName"} or {@code null} if {@code prefix} is not mapped.
     */
    @SuppressWarnings("StringEquality")
    public @Nullable String expandPrefixed(String str, int start, int colonIdx, int localNameEnd) {
        String uri = uri(str, start, colonIdx);
        if (uri == null)
            return null;
        int localBegin = colonIdx+1;
        if (uri == RDFTypes.XSD)
            return RDFTypes.fromXsdLocal(str, localBegin, localNameEnd);
        else if (uri == RDF.NS)
            return RDF.fromRdfLocal(str, localBegin, localNameEnd);
        return uri + str.substring(localBegin, localNameEnd);
    }

    /** Equivalent to {@link PrefixMap#expandPrefixed(String, int, int, int)} {@code  colon}
     *  set to the index of the first {@code :} after start and before {@code end}. */
    public @Nullable String expandPrefixed(String str, int start, int end) {
        int colon = str.indexOf(':', start);
        return colon == -1 || colon > end ? null : expandPrefixed(str, start, colon, end);
    }

    /**
     * Get the {@code uri} in the last {@code add(prefixName, uri)} call,
     * if {@code prefix} was mapped.
     */
    public @Nullable String uri(String prefixName) {
        int i = findPrefix(prefixName);
        return i < 0 ? null : data[i + (data.length>>1)];
    }

    /** Equivalent to {@code uri(str.substring(begin, colonIdx))}. */
    public @Nullable String uri(String str, int begin, int colonIdx) {
        int len = colonIdx-begin, urisStart = data.length>>1;
        for (int i = 0; i < prefixes; i++) {
            String nm = data[i];
            if (nm.length() == len && nm.regionMatches(0, str, begin, len))
                return data[i + urisStart];
        }
        return null;
    }

    private int findPrefix(String prefix) {
        for (int i = 0; i < prefixes; i++) {
            if (data[i].equals(prefix))
                return i;
        }
        return -1;
    }
}
