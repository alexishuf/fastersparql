package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.CharSequencesRow;
import com.github.alexishuf.fastersparql.operators.row.RowTestBase;

class CSOpsTest extends RowTestBase<CharSequence[], CharSequence> {
    @Override protected RowType<CharSequence[], CharSequence> ops() {
        return CharSequencesRow.INSTANCE;
    }
    @Override protected StringBuilder object1() { return new StringBuilder("<object1>"); }
}