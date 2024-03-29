package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ThreadJournalTest {

    @Test void testSingleThread() {
        if (!ENABLED) return;
        journal("garbage");
        ThreadJournal.resetJournals();
        DebugJournal.SHARED.closeAll(); // resets tick
        journal("x=", 1, "y=", 2, "z=", "one");
        journal("x=", 3, "y=", 4, "z=", "two");
        journal("x=", 5, "y=", 6, "z=", "three");
        journal("x=", 7, "y=", 8, "z=", "four");
        journal("x=", 9, "y=", 0, "z=", "five");
        StringBuilder sb = new StringBuilder();
        ThreadJournal.dumpAndReset(sb, 80);

        assertEquals("""
                      
                          |                                                                             main
                      T=0 | main journal started                                                            \s
                      T=0 | x=1 y=2 z=one                                                                   \s
                      T=1 | x=3 y=4 z=two                                                                   \s
                      T=2 | x=5 y=6 z=three                                                                 \s
                      T=3 | x=7 y=8 z=four                                                                  \s
                      T=4 | x=9 y=0 z=five                                                                  \s
                      """, sb.toString());
    }

    @Test void testConcurrent() throws Exception {
        if (!ENABLED)
            return;
        int threads = Runtime.getRuntime().availableProcessors();
        int height = DebugJournal.LINES;
        int[] last = new int[threads], expectedLast = new int[threads];
        Arrays.fill(expectedLast, height-1);
        for (int rep = 0; rep < 50; rep++) {
            ThreadJournal.resetJournals();
            DebugJournal.SHARED.closeAll(); // resets tick
            try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
                tasks.repeat(threads, id -> {
                    for (int i = 0; i < height; i++)
                        journal("thread=", id.longValue(), "i=", i);
                });
            }
            var sb = new StringBuilder();
            ThreadJournal.dumpAndReset(sb, 80);

            Arrays.fill(last, -1);
            Matcher matcher = Pattern.compile("thread=(\\d+) i=(0x)?([0-9a-f]+)").matcher(sb);
            while (matcher.find()) {
                int thread = parseInt(matcher.group(1));
                int i = parseInt(matcher.group(3), matcher.group(2) == null ? 10 : 16);
                assertEquals(last[thread]+1, i);
                ++last[thread];
            }
            assertArrayEquals(expectedLast, last);
        }
    }


}