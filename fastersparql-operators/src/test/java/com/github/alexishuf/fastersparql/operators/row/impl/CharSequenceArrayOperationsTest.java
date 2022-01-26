package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsTestBase;

class CharSequenceArrayOperationsTest extends RowOperationsTestBase {
    @Override protected RowOperationsProvider provider() {
        return new CharSequenceArrayOperations.Provider();
    }

    @Override protected Object object1() {
        return new StringBuilder("null");
    }
}