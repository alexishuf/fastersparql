package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.Throwing;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Processor} that applies a mapping function.
 *
 * @param <I>> the input type
 * @param <O>> the output type
 */
public class MappingPublisher<I, O> extends AbstractProcessor<I, O> {
    private static final Logger log = LoggerFactory.getLogger(MappingPublisher.class);

    /**
     * If true, elements for which {@link MappingPublisher#mapFunction} throws are
     * dropped and not delivered to the downstream {@link Subscriber#onNext(Object)} method.
     * The exception will be logged on an INFO level, nevertheless.
     *
     * The default is false, so that {@link Throwable}s from {@link MappingPublisher#mapFunction}
     * are delivered to downstream {@link Subscriber#onError(Throwable)} and no further
     * elements are processed.
     */
    private final boolean dropFailed;

    /**
     * A function that converts instances of {@code Input} into instances of {@code Output}
     *
     * Such function may throw anything. The {@link Throwable} will be logged or delivered
     * to the downstream {@link Subscriber#onError(Throwable)} (stopping processing of
     * further elements), depending on the value of {@link MappingPublisher#dropFailed}.
     */
    private final Throwing.Function<I, O> mapFunction;

    public MappingPublisher(FSPublisher<I> upstreamPublisher,
                            boolean dropFailed,
                            Throwing.Function<I, O> mapFunction) {
        super(upstreamPublisher);
        this.dropFailed = dropFailed;
        this.mapFunction = mapFunction;
    }

    public MappingPublisher(FSPublisher<I> upstreamPublisher,
                            Throwing.Function<I, O> mapFunction) {
        this(upstreamPublisher, false, mapFunction);
    }

    @Override protected void handleOnNext(I item) throws Exception {
        try {
            O out = mapFunction.apply(item);
            emit(out);
        } catch (Throwable t) {
            if (dropFailed)
                log.debug("{} failed for {} with {}", mapFunction, item, t);
            else
                throw t;
        }

    }
}
