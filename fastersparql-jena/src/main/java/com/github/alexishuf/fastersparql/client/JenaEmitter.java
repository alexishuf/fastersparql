package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Query;
import com.github.alexishuf.fastersparql.org.apache.jena.query.QueryFactory;
import com.github.alexishuf.fastersparql.org.apache.jena.query.TxnType;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Transactional;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Var;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.QueryExec;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.QueryExecBuilder;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.RowSet;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;


public abstract class JenaEmitter<B extends Batch<B>, E extends JenaEmitter<B, E>>
        extends TaskEmitter<B, E> {
    private static final int LIMIT_TICKS  = 1;
    private static final int DEADLINE_CHK = 0xf;
    private final Transactional transactional;
    private @Nullable QueryExec exec;
    private @MonotonicNonNull RowSet rs;
    private final Var[] jVars;
    private final int prefBatchRows;
    private JenaTermParser jenaTermParser = JenaTermParser.create().takeOwnership(this);
    private final Query originalQuery;
    private final QueryExecBuilder execFac;
    private Query currentQuery;
    private @MonotonicNonNull JenaQueryBinder binder;
    private final String displayLocation;

    protected JenaEmitter(BatchType<B> batchType, Vars vars,
                          Transactional transactional,
                          String displayLocation,
                          SparqlQuery query, QueryExecBuilder execFac) {
        super(batchType, vars, EmitterService.EMITTER_SVC, CREATED, TASK_FLAGS);
        var jQuery = QueryFactory.create(query.sparql().toString());
        // required in order to make jena remember values assigned to variables at rebind():
        jQuery.setQueryResultStar(false);
        this.transactional   = transactional;
        this.displayLocation = displayLocation;
        this.originalQuery   = jQuery;
        this.currentQuery    = jQuery;
        this.execFac         = execFac;
        this.prefBatchRows   = Math.max(1, bt.preferredRowsPerBatch(outCols));
        this.jVars           = new Var[vars.size()];
        int outIdx = 0;
        for (String str : jQuery.getResultVars())
            jVars[outIdx++] = Var.alloc(str);
    }

    @Override protected void doRelease() {
        rs             = JenaUtils.safeClose(rs);
        exec           = JenaUtils.safeClose(exec);
        jenaTermParser = Owned.safeRecycle(jenaTermParser, this);
        super.doRelease();
    }

    @Override protected void appendToSimpleLabel(StringBuilder out) {
        super.appendToSimpleLabel(out);
        out.append(' ').append(displayLocation);
        out.append('\n').append(originalQuery.toString().replace("\n", " "));
    }

    @Override public Vars bindableVars() {return vars;}

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        resetForRebind(0, LOCKED_MASK);
        try {
            if (binder == null)
                binder = new JenaQueryBinder();
            exec = JenaUtils.safeClose(exec);
            currentQuery = binder.bind(originalQuery, binding);
        } finally {
            unlock();
        }
    }

    @Override protected int produceAndDeliver(int state) {
        int rowsLimit = (int)Math.min(prefBatchRows, requested());
        if (rowsLimit <= 0)
            return state; // no rows budget
        var batch = bt.create(outCols).takeOwnership(this);
        transactional.begin(TxnType.READ);
        try {
            QueryExec exec = this.exec;
            if (exec == null) {
                this.exec = exec = execFac.query(currentQuery).build();
                if (currentQuery.isAskType()) {
                    if (exec.ask()) {
                        batch.beginPut();
                        batch.commitPut();
                    }
                    exec.close();
                    return COMPLETED;
                } else {
                    rs = exec.select();
                }
            }

            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean retry = true;
            while (rowsLimit-- > 0 && (retry=rs.hasNext())) {
                var solution = rs.next();
                batch.beginPut();
                for (int i = 0; i < outCols; i++)
                    jenaTermParser.putTerm(batch, i, solution.get(jVars[i]));
                batch.commitPut();
                if ((rowsLimit&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline)
                    break;
            }
            if (batch.rows > 0)
                deliver(batch.releaseOwnership(this));
            else
                Batch.safeRecycle(batch, this);
            return retry ? state : COMPLETED;
        } finally {
            transactional.abort();
            transactional.end();
        }
    }
}
