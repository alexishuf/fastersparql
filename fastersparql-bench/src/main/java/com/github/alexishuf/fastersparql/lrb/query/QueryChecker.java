package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.FSProperties.distinctCapacity;
import static com.github.alexishuf.fastersparql.batch.BIt.PREFERRED_MIN_BATCH;

public abstract class QueryChecker<B extends Batch<B>> extends QueryRunner.BatchConsumer {
    private static final String OK = "No errors";
    public final Vars vars;
    private final @Nullable StrongDedup<B> expected;
    private final @Nullable StrongDedup<B> observed;
    public final B unexpected;
    private int rows;
    private @Nullable String explanation;

    public QueryChecker(BatchType<B> batchType, QueryName queryName){
        super(batchType);
        vars = queryName.parsed().publicVars();
        B b = queryName.expected(batchType);
        if (b == null) {
            expected = observed = null;
            unexpected = batchType.createSingleton(vars.size());
        } else {
            expected = batchType.dedupPool.getDistinct(distinctCapacity(), b.cols);
            for (int r = 0; r < b.rows; r++)
                expected.add(b, r);
            observed = batchType.dedupPool.getDistinct(distinctCapacity(), b.cols);
            unexpected = batchType.create(PREFERRED_MIN_BATCH, b.cols, PREFERRED_MIN_BATCH*32);
        }
    }

    public boolean isValid() { return OK.equals(explanation()); }

    public String explanation() {
        if (explanation == null)
            explanation = validate();
        return explanation;
    }

    private String validate() {
        if (expected != null && observed != null) {
            int[] missing = {0};
            StringBuilder sb = new StringBuilder().append("Missing rows:");
            forEachMissing((b, r) -> {
                boolean stop = ++missing[0] >= 10;
                if (!stop) sb.append("\n  ").append(b.toString(r));
                return stop;
            });
            if (missing[0] == 0 && unexpected.rows == 0)
                return OK;
            if (missing[0] >= 10)
                sb.append("\n  +").append(missing[0]-9);
            sb.append("\n Unexpected rows: ").append(unexpected.rows);
            for (int r = 0, n = Math.min(unexpected.rows, 9); r < n; r++)
                sb.append("\n  ").append(unexpected.toString(r));
            if (unexpected.rows > 9)
                sb.append("\n +").append(unexpected.rows-9);
            return sb.toString();
        } else if (rows == 0) {
            return "Results are unknown, but got no rows";
        } else {
            return OK;
        }
    }

    public interface RowConsumer {
        boolean accept(Batch<?> batch, int row);
    }

    public void forEachMissing(RowConsumer rowConsumer) {
        if (expected == null || observed == null) return;
        expected.forEach(b -> {
            for (int r = 0, rows = b.rows; r < rows; r++) {
                if (!observed.contains(b, r) && !rowConsumer.accept(b, r)) break;
            }
        });
    }

    @Override public void start(BIt<?> it) {
        rows = 0;
        unexpected.clear();
        if (observed != null)
            observed.clear(observed.cols());
        explanation = null;
    }

    @Override public void accept(Batch<?> genericBatch) {
        @SuppressWarnings("unchecked") B b = (B)genericBatch;
        rows += b.rows;
        if (expected == null || observed == null) return;
        for (int r = 0, rows = b.rows; r < rows; r++) {
            if (!expected.contains(b, r)) unexpected.putRow(b, r);
            else                          observed.add(b, r);
        }
    }
}
