package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.BoundedBufferedBIt;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link BIt} that receives UTF-8 bytes of result sets serializations
 * ({@link ResultsParserBIt#feedShared(Rope)}) and produces rows to be
 * consumed via its {@link BIt} methods.
 *
 * <p>This is designed for concurrent use where one thread feeds bytes and
 * another consumes the rows. A slow consumer can cause the producer thread
 * to block on {@link ResultsParserBIt#feedShared(Rope)}. See the {@link BoundedBIt}
 * methods for configuring the queue size.</p>
 *
 * @param <R> the row type
 */
public abstract class ResultsParserBIt<R> extends BoundedBufferedBIt<R> {
    /** The {@link RowType} for rows produced by this {@link BIt}. */
    public final RowType<R> rowType;

    /** Implementations should use this to instantiate the parsed rows */
    protected final RowType.Builder<R> builder;

    protected final @Nullable CallbackBIt<R> destination;

    /** Interface used via SPI to discover {@link ResultsParserBIt} implementations. */
    public interface Factory extends NamedService<SparqlResultFormat> {
        SparqlResultFormat name();
        <R> ResultsParserBIt<R> create(RowType<R> rowType, Vars vars);
        <R> ResultsParserBIt<R> create(RowType<R> rowType, CallbackBIt<R> destination);
    }

    private static final NamedServiceLoader<Factory, SparqlResultFormat> NSL = new NamedServiceLoader<>(Factory.class) {
        @Override protected Factory fallback(SparqlResultFormat name) {
            throw new NoParserException(name);
        }
    };

    /** Whether {@link ResultsParserBIt#createFor(SparqlResultFormat, RowType, Vars)}
     *  can create a {@link ResultsParserBIt} for the given format */
    public static boolean supports(SparqlResultFormat fmt) {
        return NSL.has(fmt);
    }

    /**
     * Creates a new {@link ResultsParserBIt} that will parse input in the given {@code format}
     * and will output rows of type {@code R} whose {@code vars.size()} columns correspond to the
     * given {@code vars}.
     *
     * <p>If the input declares another variables set or ordering, the parser will transparently
     * project as needed to honor the variables and order determined by the given {@code vars}.</p>
     *
     * <p>Implementations of {@link ResultsParserBIt} are discovered un first use via SPI.
     * To add a new implementation, implement {@link Factory} and add the class name to an SPI
     * services file.</p>
     *
     * @param format the format of the UTF-8 segments that will be fed via
     *               {@link ResultsParserBIt#feedShared(Rope)}
     * @param rowType basic operations for the type of row that will be created
     * @param vars list of vars that will correspond to the columns in the produced rows
     *
     * @throws NoParserException if there is no {@link ResultsParserBIt} implementation
     * supporting  {@code format}.
     */
    public static <R> ResultsParserBIt<R> createFor(SparqlResultFormat format,
                                                    RowType<R> rowType, Vars vars) {
        return NSL.get(format).create(rowType, vars);
    }

    /**
     * Equivalent to {@link ResultsParserBIt#createFor(SparqlResultFormat, RowType, Vars)},
     * but rows are delivered through {@code destination.feed(R)} and {@code this}
     * {@link ResultsParserBIt} will not produce any rows but will complete when parsing
     * completes (successfully or not).
     *
     * @param format the format of the UTF-8 segments that will be fed via
     *               {@link ResultsParserBIt#feedShared(Rope)}
     * @param rowType basic operations for the type of row that will be created
     * @param destination rows will be sent only {@code destination.feed(R)} and this
     *                    {@link ResultsParserBIt} will never output the rows itself. However, the
     *                    {@link ResultsParserBIt} will complete (blocking any consumer) until
     *                    {@code destination.complete(Throwable)} has been called due to
     *                    completion of parsing or an error.
     *
     * @throws NoParserException if there is no {@link ResultsParserBIt} implementation
     *                           supporting  {@code format}.
     */
    public static <R> ResultsParserBIt<R> createFor(SparqlResultFormat format,
                                                    RowType<R> rowType,
                                                    CallbackBIt<R> destination) {
        return NSL.get(format).create(rowType, destination);
    }

    protected ResultsParserBIt(RowType<R> rowType, Vars vars) {
        super(rowType, vars);
        this.rowType = rowType;
        this.builder = rowType.builder(vars.size());
        this.destination = null;
    }

    protected ResultsParserBIt(RowType<R> rowType, CallbackBIt<R> destination) {
        super(rowType, destination.vars());
        this.rowType = rowType;
        this.builder = rowType.builder(destination.vars().size());
        this.destination = destination;
    }

    /**
     * Parse the results data in bytes {@code begin} (inclusive) to {@code end} non-inclusive in
     * {@code rope} and emit {@link Batch}es of rows, if possible.
     *
     * <p><strong>The caller REMAINS owner of {@code rope} after this method returns</strong>.
     * Therefore, bytes in {@code rope} can only be accessed during this method call and
     * implementations of this method that need to retain some bytes in {@code rope} must copy
     * those to a memory location owned by the parser.</p>
     *
     * <p>Errors due to a malformed result serialization are not thrown by this method, rather
     * the {@link Throwable}s are delivered to the downstream consumer of the rows via
     * {@link ResultsParserBIt#complete(Throwable)}. Invalid input that is the caller's
     * fault ({@code rope == null}, {@code end  < begin} or {@code begin}|{@code end} out of
     * bounds are both reported downstream and to the caller.</p>
     *
     * <p>This method should only be called by a single thread (else calls MUST be
     * synchronized among the calling threads).</p>
     *
     * @param rope non-null {@link Rope} with UTF-8 bytes representing a SPARQL results set.
     * @throws IllegalArgumentException if {@code rope == null}, {@code end < begin}, or
     *                                  {@code begin}|{@code end} are out of bounds for {@code rope}
     * @throws InvalidSparqlResultsException if the results are invalid or are not in the format
     *                                       expected by this parser
     * @throws FSCancelledException if the results representation indicates that the query was
     *                              cancelled before it could be completed.
     * @throws BItReadClosedException if {@code this.close()} was previously called.
     */
    public final void feedShared(Rope rope) {
        try {
            if (rope == null)
                throw new IllegalArgumentException("null rope");
            else if (rope.len() == 0)
                return; // no-op
            doFeedShared(rope);
        } catch (Throwable t) {
            complete(t);
            throw t;
        }
    }

    /**
     * Implement the parsing as specified by {@link ResultsParserBIt#feedShared(Rope)}.
     *
     * <p>Input parameters are already validated, but the result serialization may still be invalid.
     * Anything thrown by this method will be wrapped into an {@link InvalidSparqlResultsException}
     * (if it is not already a instance) and reported downstream to {@link BIt} consumers via
     * {@link ResultsParserBIt#complete(Throwable)}</p>
     */
    protected abstract void doFeedShared(Rope rope);

    @Override protected final void feed(Batch<R> batch) {
        if (destination != null) destination.feed(batch);
        super.feed(batch);
    }

    @Override protected void feed(R item) {
        if (destination != null) destination.feed(item);
        super.feed(item);
    }

    /**
     * Report to downstream row consumers that there ar no more rows (if {@code error==null})
     * or that an error occurred while parsing the results and no further rows will
     * be produced (if {@code error != null}).
     *
     * <p>While this is a public method, it should only be called by the thread
     * calling {@link ResultsParserBIt#feedShared(Rope)}.</p>
     *
     * <p>If {@code error} is a {@link Throwable} other than
     * {@link InvalidSparqlResultsException}, it will be wrapped as one.</p>
     *
     * @param error the error (if parsing failed) or {@code null} if parsing completed normally.
     */
    @Override public void complete(@Nullable Throwable error) {
        boolean first = !ended;
        if (error != null && !(error instanceof BItIllegalStateException)
                          && !(error instanceof FSCancelledException)
                          && !(error instanceof FSServerException)) {
            error = new InvalidSparqlResultsException(error);
        }
        try {
            super.complete(error);
        } finally {
            if (destination != null && first)
                destination.complete(error);
        }
    }
}
