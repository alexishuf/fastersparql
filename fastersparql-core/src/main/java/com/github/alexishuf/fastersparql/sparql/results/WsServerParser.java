package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Requestable;
import com.github.alexishuf.fastersparql.model.rope.Rope;

public class WsServerParser<B extends Batch<B>> extends AbstractWsParser<B> {
    private final int bindingSeqCol;
    private final WsBindingSeq seqWriter;
    private final Requestable requestable;

    public WsServerParser(CompletableBatchQueue<B> dest, Requestable requestable) {
        super(dest);
        this.bindingSeqCol = dest.vars().indexOf(WsBindingSeq.VAR);
        this.requestable   = requestable;
        this.seqWriter     = bindingSeqCol < 0 ? null : new WsBindingSeq();
    }

    @Override protected void onCancel() {
        dst.cancel(false);
    }

    @Override protected void commitRow() throws CancelledException, BatchQueue.TerminatedException {
        if (seqWriter != null)
            seqWriter.write(rowsParsed, batch, bindingSeqCol);
        super.commitRow();
    }

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        if (!rope.has(0, REQUEST))
            return false;
        requestable.request(rope.parseLong(begin + REQUEST.length));
        return true;
    }
}
