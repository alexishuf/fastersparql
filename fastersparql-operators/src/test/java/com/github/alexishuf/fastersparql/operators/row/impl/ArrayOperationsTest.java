package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperationsProvider;
import com.github.alexishuf.fastersparql.client.model.row.impl.ArrayOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsTestBase;

class ArrayOperationsTest extends RowOperationsTestBase {
    @Override protected RowOperationsProvider provider() {
        return new ArrayOperations.Provider();
    }

    @Override protected Object object1() {
        return "object1";
    }
}