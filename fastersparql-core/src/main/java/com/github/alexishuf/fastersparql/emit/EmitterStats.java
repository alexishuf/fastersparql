package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

public class EmitterStats {
    public static final boolean ENABLED = FSProperties.emitLogStats();
    public long deliveredBatches, deliveredNullBatches, deliveredRows, deliveredSingleRowBatches;
    public long receivedBatches, receivedNullBatches, receivedRows, receivedSingleRowBatches;
    public long rebinds;
    public int rebindVarsChanged;
    public int receivers;
    private Vars lastRebindVars = Vars.EMPTY;

    public static EmitterStats createIfEnabled() { return ENABLED ? new EmitterStats() : null; }

    public void onBatchDelivered(@Nullable Batch<?> b) {
        ++deliveredBatches;
        if (b == null) {
            ++deliveredNullBatches;
        } else {
            if (b.rows == 1)
                ++deliveredSingleRowBatches;
            deliveredRows += b.rows;
        }
    }

    public void onBatchReceived(@Nullable Batch<?> b) {
        ++receivedBatches;
        if (b == null) {
            ++receivedNullBatches;
        } else {
            if (b.rows == 1)
                ++receivedSingleRowBatches;
            receivedRows += b.rows;
        }
    }
    public void onBatchPassThrough(@Nullable Batch<?> b) {
        onBatchReceived(b);
        onBatchDelivered(b);
    }

    public void onRebind(BatchBinding<?> binding) {
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
}
