package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.org.apache.jena.query.ReadWrite;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Transactional;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Var;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.QueryExec;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.RowSet;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class JenaBIt<B extends Batch<B>> extends UnitaryBIt<B> {
    private static final Var[] EMPTY_VARS = new Var[0];

    private final Transactional transactional;
    private QueryExec exec;
    private boolean started, askResult;
    private final boolean ask;
    private @MonotonicNonNull RowSet rs;
    private JenaTermParser jenaTermParser = JenaTermParser.create().takeOwnership(this);
    private Var[] jVars = EMPTY_VARS;

    public JenaBIt(BatchType<B> batchType, Vars vars, Transactional transactional,
                   QueryExec exec, boolean ask) {
        super(batchType, vars);
        this.transactional = transactional;
        this.exec          = exec;
        this.ask           = ask;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            exec.close();
            exec = null;
            jenaTermParser = Owned.safeRecycle(jenaTermParser, this);
        } finally {
            super.cleanup(cause);
        }
    }

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        transactional.begin(ReadWrite.READ);
        try {
            if (!started)
                start();
            return super.nextBatch(offer);
        } finally {
            transactional.abort();
            transactional.end();
        }
    }

    private void start() {
        started = true;
        if (ask) {
            askResult = exec.ask();
        } else {
            rs = exec.select();
            jVars = rs.getResultVars().toArray(new Var[0]);
        }
    }

    @Override protected B fetch(B dst) {
        if (rs == null || !rs.hasNext()) {
            if (rs == null && askResult) {
                dst.beginPut();
                dst.commitPut();
            }
            exhausted = true;
        } else {
            var b = rs.next();
            dst.beginPut();
            for (int c = 0; c < jVars.length; c++)
                jenaTermParser.putTerm(dst, c, b.get(jVars[c]));
            dst.commitPut();
        }
        return dst;
    }
}
