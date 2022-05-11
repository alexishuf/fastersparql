package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperationsProvider;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsTestBase;

class StringArrayOperationsTest extends RowOperationsTestBase {
    @Override protected RowOperationsProvider provider() {
        return new StringArrayOperations.Provider();
    }

    @Override protected Object object1() {
        return "object1";
    }
}