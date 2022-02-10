package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NullJoinReorderStrategy;
import org.junit.jupiter.api.Test;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties.bindJoinReorder;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties.hashJoinReorder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FasterSparqlOpPropertiesTest {

    @Test
    void testDefaultBindJoinReorder() {
        assertEquals(AvoidCartesianJoinReorderStrategy.class, bindJoinReorder().getClass());
    }

    @Test
    void testBindJoinReorderFallsBackToGeneral() {
        System.setProperty(FasterSparqlOpProperties.OP_JOIN_REORDER, "Null");
        try {
            assertEquals(NullJoinReorderStrategy.class, bindJoinReorder().getClass());
        } finally {
            System.clearProperty(FasterSparqlOpProperties.OP_JOIN_REORDER);
        }
    }

    @Test
    void testHashJoinReorderOverridable() {
        System.setProperty(FasterSparqlOpProperties.OP_JOIN_REORDER, "Null");
        System.setProperty(FasterSparqlOpProperties.OP_JOIN_REORDER_HASH, "AvoidCartesian");
        try {
            assertEquals(AvoidCartesianJoinReorderStrategy.class, hashJoinReorder().getClass());
        } finally {
            System.clearProperty(FasterSparqlOpProperties.OP_JOIN_REORDER);
            System.clearProperty(FasterSparqlOpProperties.OP_JOIN_REORDER_HASH);
        }
    }

    @Test
    void testWcoJoinReorderThrowsInsteadOfFallback() {
        System.setProperty(FasterSparqlOpProperties.OP_JOIN_REORDER, "Null");
        System.setProperty(FasterSparqlOpProperties.OP_JOIN_REORDER_WCO, "bullshit");
        try {
            assertThrows(IllegalArgumentException.class, FasterSparqlOpProperties::wcoJoinReorder);
        } finally {
            System.clearProperty(FasterSparqlOpProperties.OP_JOIN_REORDER);
            System.clearProperty(FasterSparqlOpProperties.OP_JOIN_REORDER_WCO);
        }
    }
}