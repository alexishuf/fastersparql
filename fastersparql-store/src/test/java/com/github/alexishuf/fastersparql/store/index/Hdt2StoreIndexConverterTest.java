package com.github.alexishuf.fastersparql.store.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinTask;
import java.util.stream.Stream;

import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class Hdt2StoreIndexConverterTest {
    private static final Logger log = LoggerFactory.getLogger(Hdt2StoreIndexConverterTest.class);

    static Stream<Arguments> test() {
        return Stream.of(
                arguments("small.hdt", false, false),
                arguments("small.hdt", false, true),
                arguments("small.hdt", true,  false),
                arguments("small.hdt", true,  true),
                arguments("NYT.hdt", false, true)
        );
    }

    @ParameterizedTest @MethodSource
    void test(String resourcePath, boolean standalone, boolean optimizeLocality) throws Exception {
        log.info("Converting and loading {}", resourcePath);
        var converter = new Hdt2StoreIndexConverter().standaloneDict(standalone)
                                          .optimizeLocality(optimizeLocality);
        try (var tester = IterationTester.createFromHDT(getClass(), resourcePath, converter)) {
            log.info("Converted and loaded {}", resourcePath);
            ForkJoinTask<?> strings = commonPool().submit(tester::testLoadAndLookupStrings);
            tester.testIteration();
            strings.get();
        }
    }
}
