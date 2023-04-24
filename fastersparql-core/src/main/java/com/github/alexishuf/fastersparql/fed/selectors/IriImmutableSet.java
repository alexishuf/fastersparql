package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BSearch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.arraysEqual;
import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.compareTo;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOf;

final class IriImmutableSet {
    private final Term[] iris;
    private final int[] ids, offsets;

    public static final IriImmutableSet EMPTY = new IriImmutableSet(List.of());

    public IriImmutableSet(Term[] iris, int[] ids, int[] offsets) {
        this.iris = iris;
        this.ids = ids;
        this.offsets = offsets;
    }

    public IriImmutableSet(Collection<Term> iris) {
        this.ids = sortIds(iris);
        this.offsets = new int[ids.length + 1];
        this.iris = groupIris(iris, ids, offsets);
    }

    public static IriImmutableSet load(InputStream in) throws IOException {
        List<Term> iris = readIris(in);
        int[] ids = sortIds(iris);
        int[] offsets = new int[ids.length + 1];
        return new IriImmutableSet(groupIris(iris, ids, offsets), ids, offsets);
    }

    private static List<Term> readIris(InputStream in) throws IOException {
        ByteRope r = new ByteRope(2048);
        r.readLine(in);
        int lines;
        try {
            long ln = r.parseLong(0);
            if (ln < 0 || ln > Integer.MAX_VALUE)
                throw new BadSerializationException("Invalid number of IRIs: "+ln);
            lines = (int)ln;
            r.clear();
        } catch (NumberFormatException e) {
            throw new BadSerializationException("Expected number of IRIs, got"+r);
        }

        List<Term> iris = new ArrayList<>(lines);
        for (int line = 0; line < lines; line++) {
            r.clear().readLine(in);
            iris.add(Term.valueOf(r, 0, r.len));
        }
        return iris;
    }

    private static int[] sortIds(Collection<Term> iris) {
        int[] ids = new int[Math.min(256, iris.size()+1)];
        int nIds = 1; // zero is always present
        for (Term iri : iris) {
            int i = BSearch.binarySearch(ids, 0, nIds, iri.flaggedDictId);
            if (i < 0) {
                if (nIds == ids.length)
                    ids = copyOf(ids, ids.length + (ids.length >> 1));
                i = -i - 1;
                if (i < nIds)
                    arraycopy(ids, i, ids, i+1, nIds-i);
                if ((ids[i] = iri.flaggedDictId) < 0)
                    throw new IllegalArgumentException("Expected IRI, got literal");
                ++nIds;
            }
        }
        if (nIds < ids.length)
            ids = copyOf(ids, nIds);
        return ids;
    }

    private static Term[] groupIris(Collection<Term> iris, int[] ids, int[] offsets) {
        var grouped = new Term[iris.size()];
        int o = 0;
        for (int b, i = 0; i < ids.length; i++) {
            int id = ids[i];
            offsets[i] = b = o;
            for (Term t : iris) {
                if (t.flaggedDictId != id) continue;
                int idx = BSearch.binarySearchLocal(grouped, b, o, t.local);
                if (idx >= 0) continue;
                if ((idx = -idx-1) < o)
                    System.arraycopy(grouped, idx, grouped, idx+1, o-idx);
                grouped[idx] = t;
                ++o;
            }
        }
        if (o < grouped.length)
            grouped = copyOf(grouped, o);
        offsets[ids.length] = grouped.length;
        return grouped;
    }

    public void save(OutputStream out) throws IOException {
        out.write(Integer.toString(iris.length).getBytes(UTF_8));
        out.write('\n');
        for (Term term : iris) {
            int id = term.flaggedDictId;
            if (id > 0)
                out.write(RopeDict.get(id).u8());
            out.write(term.local);
            out.write('\n');
        }
    }

    public int size() { return iris.length; }

    public boolean has(Term term) {
        int fId = term.flaggedDictId;
        if (fId <= 0)
            return fId == 0 && noIdHas(term.local);
        int idIdx = BSearch.binarySearch(ids, fId);
        if (idIdx < 0) return false;
        byte[] local = term.local;
        if (local.length == 0)
            return iris[offsets[idIdx]].local.length == 0;
        byte f = local[0];
        for (int i = offsets[idIdx], e = offsets[idIdx + 1]; i < e; i++) {
            byte[] cLocal = iris[i].local;
            byte cF = cLocal[0];
            if (cF > f) break;
            if (cF == f && arraysEqual(local, cLocal)) return true;
        }
        return false;
    }

    private boolean noIdHas(byte[] iri) {
        int cmp = -1;
        for (int i = offsets[0], e = offsets[1]; i < e && cmp < 0; i++) {
            byte[] c = iris[i].local;
            cmp = compareTo(c, iri);
        }
        return cmp == 0;
    }
}
