package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.github.alexishuf.fastersparql.FSProperties.askNegativeCapacity;
import static com.github.alexishuf.fastersparql.FSProperties.askPositiveCapacity;
import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AskSelector extends Selector {
    private static final Term X = Term.valueOf("?x");
    public static final String NAME = "ask";
    private static final byte[] TYPE_LINE_U8 = (NAME+'\n').getBytes(UTF_8);
    private static final LIFOPool<TermBatch> TRIPLE_POOL
            = LIFOPool.perProcessor(TermBatch.class, 1);

    private final SparqlClient client;
    private final StrongDedup<TermBatch> positive, negative;

    private static final byte[] POSITIVE_HDR = "@POSITIVE cap=".getBytes(UTF_8);
    private static final byte[] NEGATIVE_HDR = "@NEGATIVE cap=".getBytes(UTF_8);
    private static final byte[] ZERO_LF = "0\n".getBytes(UTF_8);

    private static TermBatch triple(Term s, Term p, Term o) {
        TermBatch b = TRIPLE_POOL.get();
        if (b == null) b = TERM.createSingleton(3);
        b.beginPut();
        b.putTerm(0, s);
        b.putTerm(1, p);
        b.putTerm(2, o);
        b.commitPut();
        return b;
    }

    public static final class AskLoader implements Loader {
        @Override public String name() { return NAME; }

        @Override public Selector create(SparqlClient client, Spec spec) {
            return new AskSelector(client, spec);
        }

        @Override public Selector
        load(SparqlClient client, Spec spec, InputStream in) throws IOException, BadSerializationException {
            var r = new ByteRope(64);
            var termParser = new TermParser();
            StrongDedup<TermBatch> positive = null, negative = null, current = null;
            while (r.clear().readLine(in)) {
                if (r.get(0) == '@') {
                    current = parseHeader(r);
                    if (r.get(1) == 'P') positive = current;
                    else                 negative = current;
                } else if (current == null) {
                    throw new BadSerializationException("Expected @POSITIVE/@NEGATIVE header");
                } else {
                    parseRow(current, termParser, r);
                }
            }
            if (positive == null)
                positive = StrongDedup.strongUntil(TERM, askPositiveCapacity(), 3);
            if (negative == null)
                negative = StrongDedup.strongUntil(TERM, askNegativeCapacity(), 3);
            return new AskSelector(client, spec, positive, negative);
        }

        private static void parseRow(StrongDedup<TermBatch> dedup,
                                     TermParser termParser, ByteRope r) {
            int len = r.len;
            var t = triple(termParser.parseTerm(r, 0, len),
                           termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len),
                           termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len));
            dedup.add(t, 0);
        }

        private static StrongDedup<TermBatch> parseHeader(ByteRope r) {
            if (!r.has(0, POSITIVE_HDR) && !r.has(0, NEGATIVE_HDR))
                throw new BadSerializationException("Unexpected header");
            try {
                long cap = r.parseLong(POSITIVE_HDR.length);
                if (cap > 0 && cap < Integer.MAX_VALUE)
                    return StrongDedup.strongUntil(TERM, (int)cap, 3);
            } catch (NumberFormatException ignored) { }
            throw new BadSerializationException("Invalid capacity "+r);
        }
    }

    public AskSelector(SparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.client = client;
        this.positive = StrongDedup.strongUntil(TERM, spec.getOr("positive-capacity", askPositiveCapacity()), 3);
        this.negative = StrongDedup.strongUntil(TERM, spec.getOr("negative-capacity", askNegativeCapacity()), 3);
        notifyInit(InitOrigin.LAZY, null);
    }

    public AskSelector(SparqlClient client, Spec spec,
                       StrongDedup<TermBatch> positive, StrongDedup<TermBatch> negative) {
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

    private void saveSection(OutputStream out, byte[] hdr, Dedup<TermBatch> set)
            throws IOException {
        out.write(hdr);
        if (set == null) {
            out.write(ZERO_LF);
        } else {
            out.write(Integer.toString(set.capacity()).getBytes(UTF_8));
            out.write('\n');
            set.forEach(b -> {
                ByteRope line = new ByteRope();
                for (int r = 0, rows = b.rows; r < rows; r++) {
                    b.writeNT(line.clear(), r, 0);
                    b.writeNT(line.append(' '), r, 1);
                    b.writeNT(line.append(' '), r, 2);
                    line.append('\n').write(out);
                }
            });
        }
    }

    @Override public boolean has(TriplePattern tp) {
        Term s = tp.s.isVar() ? X : tp.s, p = tp.p.isVar() ? X : tp.p, o = tp.o.isVar() ? X : tp.o;
        int vars = ((s.isVar() ? 1 : 0)<<2) | ((p.isVar() ? 1 : 0)<<1) | (o.isVar() ? 1 : 0);
        TermBatch canonBatch = triple(s, p, o);
        Term[] canon = canonBatch.arr();
        if (positive.contains(canonBatch, 0)) return true;
        if (negative.contains(canonBatch, 0)) return false;

        // try a negative match against more general queries
        if (s != X) {
            canon[0] = X;
            if (negative.contains(canonBatch, 0)) return false;
            if (o != X) {
                canon[2] = X;
                if (negative.contains(canonBatch, 0)) return false;
                canon[2] = o;
            }
            canon[0] = s;
        }
        if (o != X) {
            canon[2] = X;
            if (negative.contains(canonBatch, 0)) return false;
            canon[2] = o;
        }
        if (p != X) {
            canon[1] = X;
            if (negative.contains(canonBatch, 0)) return false;
            canon[1] = p;
        }
        // canon == {s, p, o}

        // not in cache, issue a query using the given tp
        try (BIt<?> it = client.query(COMPRESSED, tp.toAsk()).eager()) {
            boolean has = it.nextBatch(null) != null;
            (has ? positive : negative).add(canonBatch, 0);
            if (has && (vars&1) == 0) { // if positive and ground predicate, store generalized
                if (s != X) {
                    canon[0] = X;
                    positive.add(canonBatch, 0); // store X p o
                }
                if (o != X) {
                    canon[2] = X;
                    positive.add(canonBatch, 0); // store X p X
                    if (s != X) {
                        canon[0] = s;
                        positive.add(canonBatch, 0); // store s p X
                    }
                }
            }
            return has;
        }
    }

    @Override public void close() { client.close(); }
}
