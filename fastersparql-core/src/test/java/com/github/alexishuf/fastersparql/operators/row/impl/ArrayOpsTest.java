package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.operators.row.RowTestBase;

class ArrayOpsTest extends RowTestBase<String[], String> {
    @Override protected RowType<String[], String> ops() { return new ArrayRow<>(String.class); }
    @Override protected String object1()               { return "object1"; }
}