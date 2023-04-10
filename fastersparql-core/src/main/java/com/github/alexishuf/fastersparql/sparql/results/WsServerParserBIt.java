package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;

public class WsServerParserBIt<B extends Batch<B>> extends AbstractWsParserBIt<B> {

    public WsServerParserBIt(WsFrameSender<?> frameSender, BatchType<B> batchType, Vars vars, int maxBatches) {
        super(frameSender, batchType, vars, maxBatches);
    }

    public WsServerParserBIt(WsFrameSender<?> frameSender, BatchType<B> batchType,
                             CallbackBIt<B> destination) {
        super(frameSender, batchType, destination);
    }


    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        return false;
    }
}
