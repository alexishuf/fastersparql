package com.github.alexishuf.fastersparql.operators.row.impl;

import java.util.List;

class ListMatcherTest extends ArrayMatcherTest {
    @Override protected ArrayMatcher create(List<String> leftVars, List<String> rightVars) {
        return new ListMatcher(leftVars, rightVars);
    }

    @Override protected Object toRow(List<String> list) {
        return list;
    }
}
