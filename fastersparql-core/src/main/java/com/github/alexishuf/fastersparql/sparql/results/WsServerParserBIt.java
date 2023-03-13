package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;

public class WsServerParserBIt<R> extends AbstractWsParserBIt<R> {
    public WsServerParserBIt(WsFrameSender frameSender, RowType<R> rowType,
                             CallbackBIt<R> destination) {
        super(frameSender, rowType, destination);
    }


    @Override protected boolean handleRoleSpecificControl(Rope rope, int begin, int eol) {
        return false;
    }
}
