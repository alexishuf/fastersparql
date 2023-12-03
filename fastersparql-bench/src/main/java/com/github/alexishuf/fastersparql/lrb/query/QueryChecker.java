package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.results.serializer.TsvSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public abstract class QueryChecker<B extends Batch<B>> extends QueryRunner.BatchConsumer {
    private static final String OK = "No errors";
    public final Vars vars;
    private final QueryName queryName;
    private final @Nullable StrongDedup<B> expected;
    private final @Nullable StrongDedup<B> observed;
    private final int expectedRows;
    public B unexpected;
    private int rows;
    private @Nullable Throwable error;
    private @Nullable String explanation;

    public QueryChecker(BatchType<B> batchType, QueryName queryName){
        super(batchType);
        vars = queryName.parsed().publicVars();
        this.queryName = queryName;
        B b = queryName.amputateNumbers(batchType, queryName.expected(batchType));
        if (b == null) {
            expectedRows = 0;
            expected = observed = null;
            unexpected = batchType.create(vars.size());
        } else {
            int rows = b.totalRows();
            expectedRows = rows;
            expected = StrongDedup.strongForever(batchType, rows, b.cols);
            for (var n = b; n != null; n = n.next) {
                for (int r = 0, nRows = n.rows; r < nRows; r++)
                    expected.add(n, r);
            }
            observed = StrongDedup.strongForever(batchType, rows, b.cols);
            unexpected = batchType.create(b.cols);
        }
    }

    @Override public final void finish(@Nullable Throwable error) {
        try {
            this.error = error;
            doFinish(error);
            if (error != null)
                explanation(); // generates explanation, allowing unexpected to be recycled
        } finally {
            if (unexpected != null && explanation != null)
                unexpected = unexpected.recycle();
        }
    }

    protected abstract void doFinish(@Nullable Throwable error);

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

    @SuppressWarnings("unused") private void serialize(File dest, Object rows) {
        var sink = new ByteRope();
        var serializer = new TsvSerializer();
        serializer.init(vars, vars, false, sink);
        if (rows instanceof Dedup<?> d)
            d.forEach(b -> serializer.serializeAll(b, sink));
        else if (rows instanceof Batch<?> b)
            serializer.serializeAll(b, sink);
        else
            throw new IllegalArgumentException("Unsupported type for rows="+rows);
        serializer.serializeTrailer(sink);
        List<SegmentRope> lines = new ArrayList<>();
        for (int i = 0, j; i < sink.len; i = j) {
            long sepLenAndEnd = sink.skipUntilLineBreak(i, sink.len);
            lines.add(sink.sub(i, j = (int)sepLenAndEnd + (int)(sepLenAndEnd>>32)));
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

    @Override public void start(Vars vars) {
        rows        = 0;
        explanation = null;
        if (observed != null) {
            if (unexpected == null)//noinspection unchecked
                unexpected = (B)batchType.create(vars.size());
            else
                unexpected.clear();
            observed.clear(observed.cols());
            assert observed.cols() == vars.size();
        }
    }

    @Override public void accept(Batch<?> gb) {
        //noinspection unchecked
        B amputated = queryName.amputateNumbers((BatchType<B>) batchType, (B)gb);
        for (B node = amputated; node != null; node = node.next) {
            rows += node.rows;
            if (expected == null || observed == null) return;
            for (int r = 0, rows = node.rows; r < rows; r++) {
                if (!expected.contains(node, r))
                    unexpected.putRow(node, r);
                else
                    observed.add(node, r);
            }
        }
        if (amputated != gb)
            Batch.recycle(amputated);
    }
}
