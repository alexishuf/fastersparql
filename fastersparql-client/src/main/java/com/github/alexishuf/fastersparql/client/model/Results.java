package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.AsyncIterable;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A list of variables to bundled with a {@link Publisher} that produces solutions, each
 * represented by an instance of {@code Row}.
 *
 * Any {@code Row} type should fall into one of these categories:
 * <ol>
 *     <li>It is iterable (array, {@link Iterable}, {@link Stream}, etc.)</li>
 *     <li>It maps vars to bindings ({@link Map} iterable of name-value pairs, etc)</li>
 * </ol>
 *
 * For {@code Row} types in the first category, iteration will yield as many values as
 * there are {@link Results#vars()} with the i-th value corresponding to
 * the i-th var. For unbound vars, {@code null} or {@code Optional.empty()} must be used.
 *
 * For {@code Row} types in the second category, unbound vars can be either not mapped or
 * mapped to {@code null} or {@code Optional.empty()}.
 *
 * @param <Row>  the type that represents an individual solution, binding RDF terms to variables.
 */
@Slf4j @Getter @Accessors(fluent = true)
public class Results<Row> {
    /**
     * The list of variables in the results.
     *
     * Being a {@link SafeAsyncTask} means that eventual failures before or during parsing of
     * the variable lists are not reported here. On failure, the task will complete with an
     * empty list (which is the expected value for ASK queries) and the failure cause will be
     * reported via {@link Subscriber#onError(Throwable)} by the {@link Results#publisher()}.
     *
     * If an error occurs, an empty list will be returned and the {@link Throwable}
     * will be delivered to {@link Subscriber#onError(Throwable)} by {@link Results#publisher()}
     */
    private final SafeAsyncTask<List<String>> vars;

    /**
     * The class of items produced by {@link Results#publisher()}.
     */
    private final Class<? super Row> rowClass;

    /**
     * A single-subscription {@link Publisher} that produces solutions, each providing bindings
     * to variables in {@link Results#vars()}.
     *
     * If {@code Row} is iterable (e.g., array, {@link Iterable}, {@link Stream}, etc), the i-th
     * iterated value must contain a null, {@code Optional.empty()} or an RDF term corresponding
     * to the i-th variable in {@link Results#vars()}.
     */
    private final Publisher<Row> publisher;

    public Results(SafeAsyncTask<List<String>> vars, Class<? super Row> rowClass,
                   Publisher<? extends Row> publisher) {
        this.vars = vars;
        this.rowClass = rowClass;
        //noinspection unchecked
        this.publisher = (Publisher<Row>) publisher;
    }

    /**
     * Create a {@link Results} with an empty publisher that fails with {@code cause} upon
     * subscription.
     *
     * @param rowClass the class of rows to report, even tough no row will be emitted.
     * @param cause the error to deliver via {@link Subscriber#onError(Throwable)}
     * @param <Row> the row type
     * @return a new {@link Results}.
     */
    public static <Row> Results<Row> forError(Class<? super Row> rowClass, Throwable cause) {
        return new Results<>(Async.wrap(Collections.emptyList()), rowClass,
                             new EmptyPublisher<>(cause));
    }

    /**
     * Get a lazy {@link Iterable} wrapping the {@link Publisher}.
     *
     * As {@link Results#publisher} is single-subscription, calling {@link AsyncIterable#start()}
     * will disallow subsequent calls to {@link Publisher#subscribe(Subscriber)} on
     * {@link Results#publisher()}.
     *
     * @return a non-null {@link AsyncIterable} wrapping {@link Results#publisher}
     */
    public AsyncIterable<Row> iterable() {
        return new IterableAdapter<>(publisher);
    }
}
