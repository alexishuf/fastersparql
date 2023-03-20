package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DebugJournalTest {
    @Test public void test() {
        DebugJournal j = new DebugJournal();
        var r1 = j.role("T1");
        var r2 = j.role("T2");
        var r3 = j.role("T3");
        r1.write("x=", 1, "y=", 2);
        r2.write("x", "a", "y=", "b");
        r3.write("x", 3, "y=", "c");
        r2.write("x=", 4, "y=", 5, "z=",  "d");
        assertEquals("""
                    |                   T1 |                   T2 |                   T3
                T=0 | T1 journal started   | T2 journal started   | T3 journal started \s
                T=0 | x=1 y=2              |                      |                    \s
                T=1 |                      | x a y=b              |                    \s
                T=2 |                      |                      | x 3 y=c            \s
                T=3 |                      | x=4 y=5 z=d          |                    \s
                """, j.toString(20));
    }

}