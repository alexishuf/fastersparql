package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.PooledSegmentRopeView;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.*;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.LIT;
import static com.github.alexishuf.fastersparql.sparql.results.ResultsParser.createFor;
import static java.nio.charset.StandardCharsets.UTF_8;

public enum QueryName {
    S1,
    S2,
    S3,
    S4,
    S5,
    S6,
    S7,
    S8,
    S9,
    S10,
    S11,
    S12,
    S13,
    S14,

    C1,
    C2,
    C3,
    C4,
    C5,
    C6,
    C7,
    C8,
    C9,
    C10,

    B1,
    B2,
    B3,
    B4,
    B5,
    B6,
    B7,
    B8;

    public QueryGroup group() {
        return switch (this) {
            case S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14 -> QueryGroup.S;
            case C1, C2, C3, C4, C5, C6, C7, C8, C9, C10 -> QueryGroup.C;
            case B1, B2, B3, B4, B5, B6, B7, B8 -> QueryGroup.B;
        };
    }

    private static final List<Map<BatchType<?>, Batch<?>>> name2type2expected;
    static  {
        int n = values().length;
        ArrayList<Map<BatchType<?>, Batch<?>>> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            list.add(Collections.synchronizedMap(new HashMap<>()));
        name2type2expected = list;
    }

    public <B extends Batch<B>> @Nullable B expected(BatchType<B> batchType) {
        //noinspection unchecked
        return (B) name2type2expected.get(this.ordinal())
                .computeIfAbsent(batchType, this::loadExpected);
    }
    private <B extends Batch<B>> @Nullable B loadExpected(BatchType<B> batchType) {
        //read vars
        Vars vars = new Vars.Mutable(10);
        try (var is = getClass().getResourceAsStream("results/" + name() + ".tsv");
             var header = PooledMutableRope.get()) {
            if (is == null) return null;
            if (!header.readLine(is))
                throw new IllegalStateException("Invalid results TSV resource is empty");
            for (int i = 1, j; i < header.len ; i = j+2) {
                j = header.skipUntil(i, header.len, (byte)'\t');
                if (!vars.add(FinalSegmentRope.asFinal(header, i, j)))
                    throw new IllegalStateException("Duplicate var in TSV resource file");
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException reading from resource", e);
        }

        //parse TSV
        try (var is = getClass().getResourceAsStream("results/" + name() + ".tsv");
             var parsed = new SPSCBIt<>(batchType, vars)) {
            var parser = createFor(TSV, parsed);
            assert is != null;
            Thread.startVirtualThread(() -> {
                try {
                    parser.feedShared(new FinalSegmentRope(is.readAllBytes())); // largest TSV has 9_053 results
                    parser.feedEnd();
                } catch (IOException e) {
                    parser.feedError(FSException.wrap(null, e));
                } catch (BatchQueue.CancelledException | BatchQueue.TerminatedException e) {
                    throw new RuntimeException("Unexpected "+e.getClass().getSimpleName());
                }
            });
            B acc = batchType.create(vars.size()).takeOwnership(this);
            for (B b = null; (b = parsed.nextBatch(b, this)) != null; )
                acc.copy(b);
            return acc;
        } catch (IOException e) {
            throw new RuntimeException("IOException reading from resource");
        }
    }

    private static final byte[] LF_ORDER_BY = "\nORDER BY".getBytes(UTF_8);
    public OpaqueSparqlQuery opaque() {
        String path = "queries/" + name();
        try (var is = getClass().getResourceAsStream(path)) {
            if (is == null) throw new RuntimeException("resource stream "+path+" not found");
            var sparql = new FinalSegmentRope(is.readAllBytes());
            int obBegin = sparql.skipUntil(0, sparql.len(), LF_ORDER_BY);
            if (obBegin != sparql.len()) {
                try (var unordered = PooledMutableRope.get()) {
                    unordered.append(sparql, 0, obBegin);
                    int obEnd = sparql.skipUntil(obBegin + 1, sparql.len, (byte)'\n');
                    unordered.append(sparql, obEnd, sparql.len);
                    sparql = FinalSegmentRope.asFinal(unordered);
                }
            }
            return new OpaqueSparqlQuery(sparql);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open resource stream for "+this);
        }
    }

    public Plan parsed() { return SparqlParser.parse(opaque()); }

    public boolean isAmputateNumberNoOp() {return this != C7 && this != C8 && this != C10;}

    public boolean isExpandUnicodeEscapesNoOp() {
        return switch (this) {
            case S8, S13, S14 -> false;
            default -> true;
        };
    }

    public <B extends Batch<B>> Orphan<B> expandUnicodeEscapes(Orphan<B> batch) {
        if (batch == null || isExpandUnicodeEscapesNoOp())
            return batch;
        B fixed = null;
        try (var view = PooledSegmentRopeView.ofEmpty();
             var tmp = PooledMutableRope.get()) {
            while (batch != null) {
                B out, in = batch.takeOwnership(this);
                batch = in.detachHead();
                if (hasUnicodeEscapes(in, view)) {
                    out = expandUnicodeEscapes(in, view, tmp);
                    Batch.safeRecycle(in, this);
                } else {
                    out = in;
                }
                fixed = Batch.quickAppend(fixed, this, out.releaseOwnership(this));
            }
        }
        return fixed.releaseOwnership(this);
    }

    private <B extends Batch<B>> boolean hasUnicodeEscapes(B in, PooledSegmentRopeView view) {
        for (int r = 0, rows = in.rows, cols = in.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                if (in.termType(r, c) == LIT && in.localView(r, c, view)) {
                    for (int i = 0, len = view.len; i < len; i++) {
                        i = view.skipUntilUnescaped(i, len, (byte)'\\');
                        byte u = i+4 > len ? 0 : view.get(i+1);
                        if (u == 'u' || u == 'U')
                            return true; // has unicode escape
                    }
                }
            }
        }
        return false; // no unicode escape
    }

    private <B extends Batch<B>> B expandUnicodeEscapes(B in, PooledSegmentRopeView view,
                                                        PooledMutableRope tmp) {
        B out = in.type().create(in.cols).takeOwnership(this);
        for (int r = 0, rows = in.rows, cols = in.cols; r < rows; r++) {
            out.beginPut();
            for (int c = 0; c < cols; c++) {
                if (in.termType(r, c) != LIT || !in.localView(r, c, view)) {
                    out.putTerm(c, in, r, c);
                } else {
                    tmp.clear();
                    int i = 0, len = view.len;
                    while (i < len) {
                        int old = i;
                        i = view.skipUntilUnescaped(i, len, (byte)'\\');
                        tmp.append(view, old, i);
                        int u = i+4 > len ? 0 : view.get(i+1);
                        if (u == 'u' || u == 'U') {
                            tmp.appendCodePoint(view.parseCodePoint(i));
                            i += u == 'u' ? 6 : 10;
                        } else {
                            if (i < len)
                                tmp.append('\\');
                            ++i;
                        }
                    }
                    out.putTerm(c, in.shared(r, c), tmp.segment, tmp.utf8, 0,
                                tmp.len, true);
                }
            }
            out.commitPut();
        }
        return out;
    }


    public <B extends Batch<B>> Orphan<B> amputateNumbers(Orphan<B> in) {
        if (in == null || isAmputateNumberNoOp())
            return in;
        short cols = Batch.peekColumns(in);
        var bt     = Batch.peekType(in);
        B fixed    = null;
        try (var tmp = PooledTermView.ofEmptyString();
             var rope  = PooledMutableRope.get()) {
            while (in != null) {
                B node = in.takeOwnership(this);
                in = node.detachHead();
                B amp = bt.create(cols).takeOwnership(this);
                for (int r = 0, rows = node.rows; r < rows; r++) {
                    amp.beginPut();
                    for (int c = 0; c < cols; c++) {
                        if (Term.isNumericDatatype(node.shared(r, c))) {
                            if (!node.getView(r, c, tmp))
                                throw new AssertionError("no term, but shared() != null");
                            var local = tmp.local();
                            int dot = local.skipUntil(0, local.len, (byte)'.');
                            rope.clear().append(local, 0, dot);
                            amp.putTerm(c, tmp.finalShared(), rope.segment, rope.utf8, 0,
                                        rope.len, true);
                        } else {
                            amp.putTerm(c, node, r, c);
                        }
                    }
                    amp.commitPut();
                }
                fixed = Batch.quickAppend(fixed, this, amp.releaseOwnership(this));
                node.recycle(this);
            }
        }
        return Owned.releaseOwnership(fixed, this);
    }
}
