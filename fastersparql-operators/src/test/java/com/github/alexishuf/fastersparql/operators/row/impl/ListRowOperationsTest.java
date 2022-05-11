package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperationsProvider;
import com.github.alexishuf.fastersparql.client.model.row.impl.ListOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsTestBase;

public class ListRowOperationsTest extends RowOperationsTestBase {
    @Override protected RowOperationsProvider provider() {
        return new ListOperations.Provider();
    }

    @Override protected Object object1() {
        return "object1";
    }
}
