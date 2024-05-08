package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

public class EmitterStats {
    public static final boolean ENABLED = FSProperties.emitStats();
    public static final boolean LOG_ENABLED = FSProperties.emitStatsLog();
    public long deliveredBatches, deliveredNullBatches, deliveredRows, deliveredSingleRowBatches;
    public long receivedBatches, receivedNullBatches, receivedRows, receivedSingleRowBatches;
    public long rebinds;
    public int rebindVarsChanged;
    public int receivers;
    private Vars lastRebindVars = Vars.EMPTY;

    public static EmitterStats createIfEnabled() { return ENABLED ? new EmitterStats() : null; }

    public void onBatchDelivered(@Nullable Orphan<? extends Batch<?>> orphan) {
        ++deliveredBatches;
        if (orphan == null) {
            ++deliveredNullBatches;
        } else {
            int totalRows = Batch.peekTotalRows(orphan), rows = Batch.peekRows(orphan);
            if (rows == 1 && totalRows == 1)
                ++deliveredSingleRowBatches;
            deliveredRows += totalRows;
        }
    }

    public void onBatchDelivered(@Nullable Batch<?> b) {
        ++deliveredBatches;
        if (b == null) {
            ++deliveredNullBatches;
        } else {
            if (b.rows == 1 && b.next == null)
                ++deliveredSingleRowBatches;
            deliveredRows += b.totalRows();
        }
    }

    public void onBatchReceived(@Nullable Orphan<? extends Batch<?>> orphan) {
        ++receivedBatches;
        if (orphan == null) {
            ++receivedNullBatches;
        } else {
            int rows = Batch.peekRows(orphan), totalRows = Batch.peekTotalRows(orphan);
            if (rows == 1 && totalRows == 1)
                ++receivedSingleRowBatches;
            receivedRows += totalRows;
        }
    }

    public void onBatchReceived(@Nullable Batch<?> b) {
        ++receivedBatches;
        if (b == null) {
            ++receivedNullBatches;
        } else {
            if (b.rows == 1 && b.next == null)
                ++receivedSingleRowBatches;
            receivedRows += b.totalRows();
        }
    }

    public void revertOnBatchReceived(@Nullable Orphan<? extends Batch<?>> b) {
        --receivedBatches;
        if (b == null) {
            --receivedNullBatches;
        } else {
            int rows = Batch.peekRows(b), totalRows = Batch.peekTotalRows(b);
            if (rows == 1 && totalRows == 1)
                --receivedSingleRowBatches;
            receivedRows -= totalRows;
        }
    }

    public void onBatchPassThrough(@Nullable Orphan<? extends Batch<?>> orphan) {
        onBatchReceived(orphan);
        onBatchDelivered(orphan);
    }
    public void onBatchPassThrough(@Nullable Batch<?> b) {
        onBatchReceived(b);
        onBatchDelivered(b);
    }

    public void onRebind(BatchBinding binding) {
        ++rebinds;
        if (!lastRebindVars.equals(binding.vars)) {
            ++rebindVarsChanged;
            lastRebindVars = binding.vars;
        }
    }

    public void report(Logger log, Object owner) {
        log.info("{}: delivered {} batches (of which {} were single-row and {} null) summing {} rows to {} receivers",
                 owner, deliveredBatches, deliveredSingleRowBatches, deliveredNullBatches, deliveredRows, receivers);
        if (rebinds > 0) {
            log.info("{}: got {} rebind()s, rebind vars changed {} times",
                    owner, rebinds, rebindVarsChanged);
        }
        if (receivedBatches > 0) {
            log.info("{}: received {} batches (of which {} were single-row and {} null) summing {} rows",
                     owner, receivedBatches, receivedSingleRowBatches, receivedNullBatches, receivedRows);
        }
    }

    public StringBuilder appendToLabel(StringBuilder sb) {
        sb.append("\ndelivered ").append(deliveredBatches).append(" batches, ")
                .append(deliveredRows).append(" rows");
        if (receivedBatches > 0) {
            sb.append("\n received ").append(receivedBatches).append(" batches, ")
                    .append(receivedRows).append(" rows");
        }
        if (rebinds > 0) {
            sb.append('\n').append(rebinds).append(" rebinds, vars changed ")
                    .append(rebindVarsChanged).append(" times");
        }
        return sb;
    }
}
