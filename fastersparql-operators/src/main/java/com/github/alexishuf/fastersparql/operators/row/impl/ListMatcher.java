package com.github.alexishuf.fastersparql.operators.row.impl;

import java.util.List;
import java.util.Objects;

public class ListMatcher extends ArrayMatcher {

    public ListMatcher(List<String> leftVars, List<String> rightVars) {
        super(leftVars, rightVars);
    }

    @Override public boolean matches(Object left, Object right) {
        List<?> lList = (List<?>) left, rList = (List<?>)right;
        for (int i = 0; i < leftIndices.length; i++) {
            Object lTerm = lList.get( leftIndices[i]);
            Object rTerm = rList.get(rightIndices[i]);
            if (!Objects.equals(lTerm, rTerm))
                return false;
        }
        return true;
    }
}
