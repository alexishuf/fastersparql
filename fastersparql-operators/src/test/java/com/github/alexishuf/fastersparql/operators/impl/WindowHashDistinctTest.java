package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.operators.TestHelpers;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

class WindowHashDistinctTest {

    @Test
    void testSaturateWindow() {
        RowOperations rowOps = RowOperationsRegistry.get().forClass(List.class);
        WindowHashDistinct op = new WindowHashDistinct(rowOps, 2);
        List<List<String>> input = asList(
                singletonList("_:x"),
                singletonList("_:x"), //dropped
                singletonList("_:y"),
                singletonList("_:x"), //dropped
                singletonList("_:y"), //dropped
                singletonList("_:z"),
                singletonList("_:x") //distinct in window
        );
        Plan<List<String>> plan = op.asPlan(TestHelpers.asPlan(input));
        TestHelpers.checkRows(asList(
                singletonList("_:x"),
                singletonList("_:y"),
                singletonList("_:z"),
                singletonList("_:x")
        ), TestHelpers.generateVars(input), null, plan, true);
    }

}