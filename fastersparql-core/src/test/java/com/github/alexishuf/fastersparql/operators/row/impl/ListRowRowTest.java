package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.operators.row.RowTestBase;

import java.util.List;

public class ListRowRowTest extends RowTestBase<List<String>, String> {
    @Override protected RowType<List<String>, String> ops() { return ListRow.STRING; }
    @Override protected String object1() { return "<object1>"; }
}
