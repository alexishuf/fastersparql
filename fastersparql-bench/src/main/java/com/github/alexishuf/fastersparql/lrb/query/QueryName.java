package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.*;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxRows;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
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
        try (var is = getClass().getResourceAsStream("results/" + name() + ".tsv")) {
            if (is == null) return null;
            ByteRope header = new ByteRope();
            if (!header.readLine(is))
                throw new IllegalStateException("Invalid results TSV resource is empty");
            for (int i = 1, j; i < header.len ; i = j+2) {
                j = header.skipUntil(i, header.len, '\t');
                if (!vars.add(new ByteRope(header.toArray(i, j))))
                    throw new IllegalStateException("Duplicate var in TSV resource file");
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException reading from resource", e);
        }

        //parse TSV
        try (var is = getClass().getResourceAsStream("results/" + name() + ".tsv");
             var parsed = new SPSCBIt<>(batchType, vars, queueMaxRows())) {
            var parser = createFor(TSV, parsed);
            assert is != null;
            Thread.startVirtualThread(() -> {
                try {
                    parser.feedShared(new ByteRope(is.readAllBytes())); // largest TSV has 9_053 results
                    parser.feedEnd();
                } catch (IOException e) {
                    parser.feedError(FSException.wrap(null, e));
                } catch (BatchQueue.CancelledException | BatchQueue.TerminatedException e) {
                    throw new RuntimeException("Unexpected "+e.getClass().getSimpleName());
                }
            });
            B acc = batchType.create(64, vars.size());
            for (B b = null; (b = parsed.nextBatch(b)) != null; )
                acc = acc.put(b);
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
            var sparql = new ByteRope(is.readAllBytes());
            int obBegin = sparql.skipUntil(0, sparql.len(), LF_ORDER_BY);
            if (obBegin != sparql.len()) {
                var unordered = new ByteRope(sparql.len);
                unordered.append(sparql, 0, obBegin);
                int obEnd = sparql.skipUntil(obBegin+1, sparql.len, '\n');
                unordered.append(sparql, obEnd, sparql.len);
                sparql = unordered;
            }
            return new OpaqueSparqlQuery(sparql);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open resource stream for "+this);
        }
    }

    public Plan parsed() { return SparqlParser.parse(opaque()); }

    public <B extends Batch<B>> B amputateNumbers(BatchType<B> type, B b) {
        return b == null ? null : amputateNumbers(type, b, 0, b.rows);
    }

    public <B extends Batch<B>> B amputateNumbers(BatchType<B> type, B b, int beginRow, int endRow) {
        if (this != C7 && this != C8 && this != C10)
            return b;
        boolean changed = false;
        B fixed = type.create(b.rows, b.cols);
        fixed.reserveAddLocals(b.localBytesUsed());
        var tmp = Term.pooledMutable();
        var tr = new ByteRope();
        for (int r = beginRow, cols = b.cols; r < endRow; r++) {
            fixed = fixed.beginPut();
            for (int c = 0; c < cols; c++) {
                if (Term.isNumericDatatype(b.shared(r, c))) {
                    changed = true;
                    if (!b.getView(r, c, tmp))
                        throw new AssertionError("no term, but shared() != null");
                    SegmentRope local = tmp.local();
                    int dot = local.skipUntil(0, local.len, '.');
                    tr.clear().append(local, 0, dot);
                    fixed.putTerm(c, tmp.shared(), tr.utf8, 0, tr.len, true);
                } else {
                    fixed.putTerm(c, b, r, c);
                }
            }
            fixed.commitPut();
        }
        tmp.recycle();
        if (changed)
            b = b.clear(b.cols).put(fixed);
        fixed.recycle();
        return b;
    }
}
