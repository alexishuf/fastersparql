package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;

public class WsServerParserBIt<B extends Batch<B>> extends AbstractWsParserBIt<B> {
    private final int bindingSeqCol;
    private final WsBindingSeq seqWriter;

    public WsServerParserBIt(WsFrameSender<?> frameSender, BatchType<B> batchType, Vars vars, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
        bindingSeqCol = vars.indexOf(WsBindingSeq.VAR);
        seqWriter = bindingSeqCol < 0 ? null : new WsBindingSeq();
    }

   @Override protected void emitRow() {
        if (seqWriter != null)
            seqWriter.write(rowsEmitted, rowBatch, bindingSeqCol);
        super.emitRow();
    }

    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        return false;
    }
}
