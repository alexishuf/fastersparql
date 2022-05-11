package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperationsProvider;
import com.github.alexishuf.fastersparql.client.model.row.impl.CharSequenceArrayOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsTestBase;

class CharSequenceArrayOperationsTest extends RowOperationsTestBase {
    @Override protected RowOperationsProvider provider() {
        return new CharSequenceArrayOperations.Provider();
    }

    @Override protected Object object1() {
        return new StringBuilder("null");
    }
}