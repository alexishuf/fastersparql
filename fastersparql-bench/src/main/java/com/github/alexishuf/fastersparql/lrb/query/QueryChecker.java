package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.TsvSerializer;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.TempOwner;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.dedup.Dedup.strongForever;

public abstract class QueryChecker<B extends Batch<B>>
        extends QueryRunner.BatchConsumer<B, QueryChecker<B>> {
    private static final String OK = "No errors";
    public final Vars vars;
    private final QueryName queryName;
    private @Nullable StrongDedup<B> expected;
    private @Nullable StrongDedup<B> observed;
    private final int expectedRows;
    public B unexpected;
    private int rows;
    private @Nullable Throwable error;
    private @Nullable String explanation;

    public QueryChecker(BatchType<B> batchType, QueryName queryName){
        super(batchType);
        this.queryName = queryName;
        B original     = queryName.expected(batchType);
        vars           = queryName.parsed().publicVars();
        expectedRows   = original == null ? 0 : original.totalRows();
        init(original);
    }

    @Override public @Nullable QueryChecker<B> recycle(Object currentOwner) {
        super.recycle(currentOwner);
        expected = Owned.safeRecycle(expected, this);
        observed = Owned.safeRecycle(observed, this);
        unexpected = Owned.safeRecycle(unexpected, this);
        return null;
    }

    @Override public final void finish(@Nullable Throwable error) {
        try {
            this.error = error;
            doFinish(error);
            if (error != null)
                explanation(); // generates explanation, allowing unexpected to be recycled
        } finally {
            if (unexpected != null && explanation != null)
                unexpected = unexpected.recycle(this);
        }
    }

    protected abstract void doFinish(@Nullable Throwable error);

    public int rows() { return rows; }

    public boolean isValid() { return OK.equals(explanation()); }

    public String explanation() {
        if (explanation == null)
            explanation = validate();
        return explanation;
    }

    private String validate() {
        if (error != null) {
            var sb = new StringBuilder().append("query execution failed with");
            sb.append(error.getClass().getSimpleName());
            var bos = new ByteArrayOutputStream();
            try (var ps = new PrintStream(bos)) {
                error.printStackTrace(ps);
            }
            sb.append(bos.toString(StandardCharsets.UTF_8));
            return sb.toString();
        } else if (expected != null && observed != null) {
            int[] missing = {0};
            StringBuilder sb = new StringBuilder()
                    .append("Expected rows: ").append(expectedRows)
                    .append("\nMissing rows:");
            forEachMissing((b, r) -> {
                boolean stop = ++missing[0] >= 10;
                if (!stop) sb.append("\n  ").append(b.toString(r));
                return !stop;
            });
            if (missing[0] == 0)
                sb.append(" 0");
            if (missing[0] == 0 && unexpected.rows == 0)
                return OK;
            if (missing[0] >= 10)
                sb.append("\n  +").append(missing[0]-9);
            sb.append("\nUnexpected rows: ").append(unexpected.totalRows());
            for (int r = 0, n = Math.min(unexpected.rows, 9); r < n; r++)
                sb.append("\n  ").append(unexpected.toString(r));
            if (unexpected.rows > 9)
                sb.append("\n +").append(unexpected.totalRows()-9);
            //serialize(new File("/tmp/expected.tsv"), expected);
            //serialize(new File("/tmp/observed.tsv"), observed);
            //serialize(new File("/tmp/unexpected.tsv"), unexpected);
            return sb.toString();
        } else if (rows == 0) {
            return "Results are unknown, but got no rows";
        } else {
            return OK;
        }
    }

     @SuppressWarnings({"unchecked", "unused", "RedundantCast"})
     private void serialize(File dest, Object rows) {
        List<FinalSegmentRope> lines = new ArrayList<>();
        ResultsSerializer.ChunkConsumer<FinalSegmentRope> chunkConsumer = lines::add;
        try (var sink = PooledMutableRope.get();
             var serializerGuard = new Guard<TsvSerializer>(this)) {
            var serializer = serializerGuard.set(TsvSerializer.create());
            serializer.init(vars, vars, false);
            serializer.serializeHeader(sink);
            if (rows instanceof Dedup<?, ?> d) {
                ((Dedup<B, ?>)d).forEach(b -> {
                    try (var tmp = new TempOwner<>(b);
                         var reassemble = new ResultsSerializer.Reassemble<B>()) {
                        serializer.serialize(tmp.releaseOwnership(), sink, Integer.MAX_VALUE,
                                reassemble, chunkConsumer);
                        tmp.restoreOwnership(reassemble.take());
                    }
                });
            } else if (rows instanceof Batch<?> b) {
                try (var tmp = new TempOwner<>((B) b);
                     var reassemble = new ResultsSerializer.Reassemble<B>()) {
                    serializer.serialize(tmp.releaseOwnership(), sink, Integer.MAX_VALUE,
                            reassemble, chunkConsumer);
                    tmp.restoreOwnership(reassemble.take());
                }
            } else {
                throw new IllegalArgumentException("Unsupported type for rows="+rows);
            }
            serializer.serializeTrailer(sink);
            lines.add(sink.take());
        }
        lines.sort(Comparator.naturalOrder());
        try (var out = new FileOutputStream(dest)) {
            for (int i = 0, n = vars.size(); i < n; i++) {
                if (i > 0) out.write('\t');
                out.write('?');
                vars.get(i).write(out);
            }
            out.write('\n');
            for (SegmentRope line : lines)
                line.write(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public interface RowConsumer {
        boolean accept(Batch<?> batch, int row);
    }

    public void forEachMissing(RowConsumer rowConsumer) {
        if (expected == null || observed == null) return;
        expected.forEach(b -> {
            for (var n = b; n != null; n = n.next) {
                for (int r = 0, rows = n.rows; r < rows; r++) {
                    if (!observed.contains(n, r) && !rowConsumer.accept(n, r)) break;
                }
            }
        });
    }

    @Override protected void start0(Vars vars) {
        assert this.vars.equals(vars);
        init(null);
    }

    private void init(@Nullable B original) {
        rows = 0;
        explanation = null;
        int cols = vars.size();
        if (unexpected == null)
            unexpected = batchType.create(cols).takeOwnership(this);
        else
            unexpected.clear(cols);
        if (observed == null || expected == null) {
            if (original == null)
                original = queryName.expected(batchType);
            if (original != null) {
                if (expected == null)
                    initExpected(original);
                int exRows = original.totalRows();
                if (observed == null)
                    observed = strongForever(batchType, exRows, cols).takeOwnership(this);
                else
                    observed.clear(cols);
            } else {
                expected = Owned.recycle(expected, this);
                observed = Owned.recycle(observed, this);
            }
        }
    }

    private void initExpected(B original) {
        B sanitized = queryName.isAmputateNumberNoOp() ? original
                : queryName.amputateNumbers(original.dup()).takeOwnership(this);
        assert expectedRows == original.totalRows();
        expected = strongForever(batchType, rows, sanitized.cols).takeOwnership(this);
        for (var n = sanitized; n != null; n = n.next) {
            for (int r = 0, nRows = n.rows; r < nRows; r++)
                expected.add(n, r);
        }
        if (sanitized != original)
            sanitized.recycle(this);
    }

    @Override public void onBatch(Orphan<B> orphan) {
        orphan = queryName.amputateNumbers(orphan);
        orphan = queryName.expandUnicodeEscapes(orphan);
        B b    = queryName.unescapeSlash(orphan).takeOwnership(this);
        try {
            check(b);
        } finally {
            b.recycle(this);
        }
    }

    @Override public void onBatchByCopy(B batch) {
        if (queryName.isAmputateNumberNoOp() && queryName.isExpandUnicodeEscapesNoOp())
            check(batch);
        else
            super.onBatchByCopy(batch);
    }

    private void check(B batch) {
        for (B node = batch; node != null; node = node.next) {
            rows += node.rows;
            if (expected == null || observed == null) return;
            for (int r = 0, rows = node.rows; r < rows; r++) {
                if (!expected.contains(node, r))
                    unexpected.putRow(node, r);
                else
                    observed.add(node, r);
            }
        }
    }
}
