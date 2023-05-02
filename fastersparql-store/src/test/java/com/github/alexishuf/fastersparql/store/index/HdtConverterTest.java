package com.github.alexishuf.fastersparql.store.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinTask;

import static java.util.concurrent.ForkJoinPool.commonPool;

public class HdtConverterTest {
    private static final Logger log = LoggerFactory.getLogger(HdtConverterTest.class);

    @ParameterizedTest @ValueSource(strings = {"small.hdt", "NYT.hdt"})
    void test(String resourcePath) throws Exception {
        log.info("Converting and loading {}", resourcePath);
        try (var tester = IterationTester.createFromHDT(getClass(), resourcePath)) {
            log.info("Converted and loaded {}", resourcePath);
            ForkJoinTask<?> strings = commonPool().submit(tester::testLoadAndLookupStrings);
            tester.testIteration();
            strings.get();
        }
    }
}
