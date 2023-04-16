package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class QueryNameTest {

    @ParameterizedTest @EnumSource(QueryName.class)
    public void testOpaqueQuery(QueryName name) {
        OpaqueSparqlQuery q = name.opaque();
        assertNotNull(q);
        assertTrue(q.sparql().len > 0);
        assertFalse(q.publicVars().isEmpty());
    }

    @ParameterizedTest @EnumSource(QueryName.class)
    public void testParseAllQueries(QueryName name) {
        Plan plan = name.parsed();
        assertNotNull(plan);
        assertTrue(plan.publicVars().size() > 0);
        assertTrue(plan.allVars().size() > 0);
        assertEquals(plan, name.parsed());
    }

    static Stream<Arguments> testParseResults() {
        List<Arguments> list = new ArrayList<>();
        for (QueryName name : QueryName.values()) {
            for (var type : List.of(Batch.TERM, Batch.COMPRESSED))
                list.add(arguments(name, type));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    public <B extends Batch<B>> void testParseResults(QueryName name, BatchType<B> type) {
        B expected = name.expected(type);
        if (expected == null) return;
        assertTrue(expected.rows > 0);
        assertEquals(name.parsed().publicVars().size(), expected.cols);
        for (int r = 0; r < expected.rows; r++) {
            boolean allNull = true;
            for (int c = 0; allNull && c < expected.cols; c++)
                allNull = expected.termType(r, c) == null;
            assertFalse(allNull, "r="+r);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"S1", "S2", "S7", "S9", "S10", "S11", "S12", "S14",
                            "C2", "C8", "C7"})
    public void testHasResults(String nameString) {
        var name = QueryName.valueOf(nameString);
        for (var type : List.of(Batch.TERM, Batch.COMPRESSED)) {
            //noinspection unchecked,rawtypes
            Batch<?> expected = name.expected((BatchType) type);
            assertNotNull(expected);
            assertTrue(expected.rows > 0);
        }
    }
}