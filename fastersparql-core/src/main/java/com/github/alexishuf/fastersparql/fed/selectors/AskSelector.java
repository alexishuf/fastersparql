package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;

import java.io.*;
import java.util.List;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.FSProperties.askNegativeCapacity;
import static com.github.alexishuf.fastersparql.FSProperties.askPositiveCapacity;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AskSelector extends Selector {
    private static final Term X = Term.valueOf("?x");
    public static final String NAME = "ask";
    public static final String STATE_FILE = "file";
    public static final List<String> STATE_FILE_P = List.of(STATE, STATE_FILE);
    private static final byte[] TYPE_LINE_U8 = (NAME+'\n').getBytes(UTF_8);
    private static final Supplier<TermBatch> TRIPLE_FACTORY = new Supplier<>() {
        @Override public TermBatch get() {return TERM.create(3).takeOwnership(RECYCLED);}
        @Override public String toString() {return "TRIPLE_POOL_FAC";}
    };
    private static final Alloc<TermBatch> TRIPLE;

    static {
        var tmp = TERM.create(3).takeOwnership(AskSelector.class);
        int bytes = tmp.totalBytesCapacity();
        tmp.recycle(AskSelector.class);
        TRIPLE = new Alloc<>(TermBatch.class, "AskSelector.TRIPLE",
                Alloc.THREADS*8, TRIPLE_FACTORY, bytes);
        Primer.INSTANCE.sched(TRIPLE::prime);
    }

    private final SparqlClient client;
    private final SparqlClient.Guard clientGuard;
    private final StrongDedup<TermBatch> positive, negative;

    private static final byte[] POSITIVE_HDR = "@POSITIVE cap=".getBytes(UTF_8);
    private static final byte[] NEGATIVE_HDR = "@NEGATIVE cap=".getBytes(UTF_8);
    private static final byte[] ZERO_LF = "0\n".getBytes(UTF_8);

    private static Orphan<TermBatch> triple(Term s, Term p, Term o) {
        var b = TRIPLE.create();
        b.rows = 1;
        Term[] a = b.arr();
        a[0] = s;
        a[1] = p;
        a[2] = o;
        return b.releaseOwnership(RECYCLED);
    }

    private static void recycleTriple(TermBatch triple, Object currentOwner) {
        triple.transferOwnership(currentOwner, RECYCLED);
        if (TRIPLE.offer(triple) != null)
            triple.recycle(RECYCLED); // return to general pool or mark as garbage
    }

    public static final class AskLoader implements Loader {
        @Override public String name() { return NAME; }

        @Override public Selector
        load(SparqlClient client, Spec spec) throws IOException, BadSerializationException {
            File file = spec.getFile(STATE_FILE_P, null);
            if (file == null || !file.exists() || file.length() == 0)
                return new AskSelector(client, spec);
            try (var r = PooledMutableRope.getWithCapacity(64);
                 var in = new FileInputStream(file);
                 var termParserGuard = new Guard<TermParser>(this)) {
                var termParser = termParserGuard.set(TermParser.create());
                if (!r.clear().readLine(in) || !r.trim().toString().equalsIgnoreCase(NAME))
                    throw new BadSerializationException.SelectorTypeMismatch(NAME, r.toString());
                Orphan<StrongDedup<TermBatch>> positive = null, negative = null;
                StrongDedup<TermBatch> current = null;
                while (r.clear().readLine(in)) {
                    if (r.get(0) == '@') {
                        current = parseHeader(r).takeOwnership(this);
                        if (r.get(1) == 'P') positive = current.releaseOwnership(this);
                        else                 negative = current.releaseOwnership(this);
                    } else if (current == null) {
                        throw new BadSerializationException("Expected @POSITIVE/@NEGATIVE header");
                    } else {
                        parseRow(current, termParser, r);
                    }
                }
                if (positive == null)
                    positive = Dedup.strongUntil(TERM, askPositiveCapacity(), 3);
                if (negative == null)
                    negative = Dedup.strongUntil(TERM, askNegativeCapacity(), 3);
                return new AskSelector(client, spec, positive, negative);
            }
        }

        private static void parseRow(StrongDedup<TermBatch> dedup,
                                     TermParser termParser, MutableRope r) {
            int len = r.len;
            var t = triple(termParser.parseTerm(r, 0, len),
                           termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len),
                           termParser.parseTerm(r, r.skipWS(termParser.termEnd(), len), len))
                    .takeOwnership(PARSE_ROW);
            dedup.add(t, 0);
            recycleTriple(t, PARSE_ROW);
        }
        private static final StaticMethodOwner PARSE_ROW = new StaticMethodOwner("AskSelector.parseRow");

        private static Orphan<StrongDedup<TermBatch>> parseHeader(MutableRope r) {
            if (!r.has(0, POSITIVE_HDR) && !r.has(0, NEGATIVE_HDR))
                throw new BadSerializationException("Unexpected header");
            try {
                long cap = r.parseLong(POSITIVE_HDR.length);
                if (cap > 0 && cap < Integer.MAX_VALUE)
                    return Dedup.strongUntil(TERM, (int)cap, 3);
            } catch (NumberFormatException ignored) { }
            throw new BadSerializationException("Invalid capacity "+r);
        }
    }

    public AskSelector(SparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.client = client;
        this.clientGuard = client.retain();
        this.positive = Dedup.strongUntil(TERM, spec.getOr("positive-capacity", askPositiveCapacity()), 3).takeOwnership(this);
        this.negative = Dedup.strongUntil(TERM, spec.getOr("negative-capacity", askNegativeCapacity()), 3).takeOwnership(this);
        notifyInit(InitOrigin.LAZY, null);
    }

    public AskSelector(SparqlClient client, Spec spec,
                       Orphan<StrongDedup<TermBatch>> positive,
                       Orphan<StrongDedup<TermBatch>> negative) {
        super(client.endpoint(), spec);
        this.client = client;
        this.clientGuard = client.retain();
        this.positive = positive.takeOwnership(this);
        this.negative = negative.takeOwnership(this);
        notifyInit(InitOrigin.LOAD, null);
    }

    @Override public void saveIfEnabled() throws IOException {
        File dest = spec.getFile(STATE_FILE_P, null);
        if (dest == null) return;
        try (var out = new FileOutputStream(dest)) {
            out.write(TYPE_LINE_U8);
            saveSection(out, POSITIVE_HDR, positive);
            saveSection(out, NEGATIVE_HDR, negative);
        }
    }

    private void saveSection(OutputStream out, byte[] hdr, Dedup<TermBatch, ?> set)
            throws IOException {
        out.write(hdr);
        if (set == null) {
            out.write(ZERO_LF);
        } else {
            out.write(Integer.toString(set.capacity()).getBytes(UTF_8));
            out.write('\n');
            set.forEach(root -> {
                try (var line = PooledMutableRope.get()) {
                    for (var b = root; b != null; b = b.next) {
                        for (int r = 0, rows = b.rows; r < rows; r++) {
                            b.writeNT(line.clear(), r, 0);
                            b.writeNT(line.append(' '), r, 1);
                            b.writeNT(line.append(' '), r, 2);
                            line.append('\n').write(out);
                        }
                    }
                }
            });
        }
    }

    @Override public boolean has(TriplePattern tp) {
        Term s = tp.s.type() == Term.Type.VAR ? X : tp.s;
        Term p = tp.p.type() == Term.Type.VAR ? X : tp.p;
        Term o = tp.o.type() == Term.Type.VAR ? X : tp.o;
        int vars = ((s == X ? 1 : 0)<<2) | ((p == X ? 1 : 0)<<1) | (o == X ? 1 : 0);
        TermBatch canonBatch = triple(s, p, o).takeOwnership(this);
        try {
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
            var em = client.emit(COMPRESSED, tp.toAsk(), Vars.EMPTY);
            boolean has = Emitters.ask(em);
            (has ? positive : negative).add(canonBatch, 0);
            if (has && (vars & 2) == 0) { // if positive and ground predicate, store generalized
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
        } finally {
            recycleTriple(canonBatch, this);
        }
    }

    @Override public void close() {
        clientGuard.close();
        Owned.safeRecycle(positive, this);
        Owned.safeRecycle(negative, this);
    }
}
