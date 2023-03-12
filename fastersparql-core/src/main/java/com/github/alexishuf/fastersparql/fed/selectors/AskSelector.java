package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.model.row.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.github.alexishuf.fastersparql.FSProperties.askNegativeCapacity;
import static com.github.alexishuf.fastersparql.FSProperties.askPositiveCapacity;
import static com.github.alexishuf.fastersparql.model.row.RowType.ARRAY;
import static com.github.alexishuf.fastersparql.model.row.RowType.COMPRESSED;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AskSelector extends Selector {
    private static final Term X = Term.valueOf("?x");
    public static final String NAME = "ask";
    private static final byte[] TYPE_LINE_U8 = (NAME+'\n').getBytes(UTF_8);

    private final SparqlClient client;
    private final StrongDedup<Term[]> positive, negative;
    private static final byte[] POSITIVE_HDR = "@POSITIVE cap=".getBytes(UTF_8);
    private static final byte[] NEGATIVE_HDR = "@NEGATIVE cap=".getBytes(UTF_8);
    private static final byte[] ZERO_LF = "0\n".getBytes(UTF_8);

    public static final class AskLoader implements Loader {
        @Override public String name() { return NAME; }

        @Override public Selector create(SparqlClient client, Spec spec) {
            return new AskSelector(client, spec);
        }

        @Override public Selector
        load(SparqlClient client, Spec spec, InputStream in) throws IOException, BadSerializationException {
            var r = new ByteRope(64);
            var termParser = new TermParser();
            StrongDedup<Term[]> positive = null, negative = null, current = null;
            while (r.clear().readLine(in)) {
                if (r.get(0) == '@') {
                    current = parseHeader(r);
                    if (r.get(1) == 'P') positive = current;
                    else                 negative = current;
                } else if (current == null) {
                    throw new BadSerializationException("Expected @POSITIVE/@NEGATIVE header");
                } else {
                    current.add(parseRow(termParser, r));
                }
            }
            if (positive == null) positive = StrongDedup.strongUntil(ARRAY, askPositiveCapacity());
            if (negative == null) negative = StrongDedup.strongUntil(ARRAY, askNegativeCapacity());
            return new AskSelector(client, spec, positive, negative);
        }

        private static Term[] parseRow(TermParser termParser, ByteRope r) {
            int len = r.len;
            return new Term[] {
                    termParser.parseTerm(r, 0, len),
                    termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len),
                    termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len)
            };
        }

        private static StrongDedup<Term[]> parseHeader(ByteRope r) {
            if (!r.has(0, POSITIVE_HDR) && !r.has(0, NEGATIVE_HDR))
                throw new BadSerializationException("Unexpected header");
            try {
                long cap = r.parseLong(POSITIVE_HDR.length);
                if (cap > 0 && cap < Integer.MAX_VALUE)
                    return StrongDedup.strongUntil(ARRAY, (int)cap);
            } catch (NumberFormatException ignored) { }
            throw new BadSerializationException("Invalid capacity "+r);
        }
    }

    public AskSelector(SparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.client = client;
        this.positive = StrongDedup.strongUntil(ARRAY, spec.getOr("positive-capacity", askPositiveCapacity()));
        this.negative = StrongDedup.strongUntil(ARRAY, spec.getOr("negative-capacity", askNegativeCapacity()));
        notifyInit(InitOrigin.LAZY, null);
    }

    public AskSelector(SparqlClient client, Spec spec,
                       StrongDedup<Term[]> positive, StrongDedup<Term[]> negative) {
        super(client.endpoint(), spec);
        this.client = client;
        this.positive = positive;
        this.negative = negative;
        notifyInit(InitOrigin.LOAD, null);
    }

    @Override public void save(OutputStream out) throws IOException {
        out.write(TYPE_LINE_U8);
        saveSection(out, POSITIVE_HDR, positive);
        saveSection(out, NEGATIVE_HDR, negative);
    }

    private void saveSection(OutputStream out, byte[] hdr, Dedup<Term[]> set)
            throws IOException {
        out.write(hdr);
        if (set == null) {
            out.write(ZERO_LF);
        } else {
            out.write(Integer.toString(set.capacity()).getBytes(UTF_8));
            out.write('\n');
            set.forEach(r -> {
                r[0].write(out);
                out.write(' ');
                r[1].write(out);
                out.write(' ');
                r[2].write(out);
                out.write('\n');
            });
        }
    }

    @Override public boolean has(TriplePattern tp) {
        Term s = tp.s.isVar() ? X : tp.s, p = tp.p.isVar() ? X : tp.p, o = tp.o.isVar() ? X : tp.o;
        int vars = ((s.isVar() ? 1 : 0)<<2) | ((p.isVar() ? 1 : 0)<<1) | (o.isVar() ? 1 : 0);
        Term[] canon = new Term[] { s, p, o };
        if (positive.contains(canon)) return true;
        if (negative.contains(canon)) return false;

        // try a negative match against more general queries
        if (s != X) {
            canon[0] = X;
            if (negative.contains(canon)) return false;
            if (o != X) {
                canon[2] = X;
                if (negative.contains(canon)) return false;
                canon[2] = o;
            }
            canon[0] = s;
        }
        if (o != X) {
            canon[2] = X;
            if (negative.contains(canon)) return false;
            canon[2] = o;
        }
        if (p != X) {
            canon[1] = X;
            if (negative.contains(canon)) return false;
            canon[1] = p;
        }

        // not in cache, issue a query using the given tp
        try (BIt<byte[]> it = client.query(COMPRESSED, tp.toAsk())) {
            boolean has = it.nextBatch().size > 0;
            (has ? positive : negative).add(canon);
            if (has && (vars&1) == 0) { // if positive and ground predicate, store generalized
                if (s != X) positive.add(new Term[]{X, p, o});
                if (o != X) {
                    positive.add(new Term[]{s, p, X});
                    if (s != X) positive.add(new Term[]{X, p, X});
                }
            }
            return has;
        }
    }

    @Override public void close() { client.close(); }
}
