package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

/**
 * A {@link BIt} that receives UTF-8 bytes of result sets serializations
 * ({@link ResultsParser#feedShared(SegmentRope)}) and produces rows to be
 * consumed via its {@link BIt} methods.
 *
 * <p>This is designed for concurrent use where one thread feeds bytes and
 * another consumes the rows. A slow consumer can cause the producer thread
 * to block on {@link ResultsParser#feedShared(SegmentRope)}. See the {@link CallbackBIt}
 * methods for configuring the queue size.</p>
 *
 * @param <B> the row type
 */
public abstract class ResultsParser<B extends Batch<B>> {
    private static final VarHandle TERMINATED;
    static {
        try {
            TERMINATED = MethodHandles.lookup().findVarHandle(ResultsParser.class, "plainTerminated", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ResultsParser.class);

    protected B batch;
    protected long rowsParsed;
    protected final CompletableBatchQueue<B> dst;
    protected boolean incompleteRow;
    private boolean eager;
    @SuppressWarnings("unused") private boolean plainTerminated;
    private final BatchType<B> batchType;
    private final int outCols;

    /** Interface used via SPI to discover {@link ResultsParser} implementations. */
    public interface Factory extends NamedService<SparqlResultFormat> {
        <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> destination);
    }

    private static final NamedServiceLoader<Factory, SparqlResultFormat> NSL = new NamedServiceLoader<>(Factory.class) {
        @Override protected Factory fallback(SparqlResultFormat name) {
            throw new NoParserException(name);
        }
    };

    /** Whether {@link ResultsParser#createFor(SparqlResultFormat, CompletableBatchQueue)}
     *  can create a {@link ResultsParser} for the given format */
    public static boolean supports(SparqlResultFormat fmt) {
        return NSL.has(fmt);
    }

    /**
     * Creates a new {@link ResultsParser} that will parse input in the given {@code format}
     * and will output rows of type {@code R} whose {@code vars.size()} columns correspond to the
     * given {@code vars}.
     *
     * <p>If the input declares another variables set or ordering, the parser will transparently
     * project as needed to honor the variables and order determined by
     * {@code destination.vars()}.</p>
     *
     * <p>Implementations of {@link ResultsParser} are discovered un first use via SPI.
     * To add a new implementation, implement {@link Factory} and add the class name to an SPI
     * services file.</p>
     *
     * @param format     the format of the UTF-8 segments that will be fed via
     *                   {@link ResultsParser#feedShared(SegmentRope)}
     * @param destination A {@link CompletableBatchQueue} that will receive the parsed batches in
     *                    {@link BatchQueue#offer(Batch)} and which will be
     *                    {@link CompletableBatchQueue#complete(Throwable)} when the parsing is
     *                    complete or if there is a parse error.
     * @throws NoParserException if there is no {@link ResultsParser} implementation
     *                           supporting  {@code format}.
     */

    public static <B extends Batch<B>> ResultsParser<B>
    createFor(SparqlResultFormat format, CompletableBatchQueue<B> destination) {
        return NSL.get(format).create(destination);
    }


    protected ResultsParser(CompletableBatchQueue<B> dst) {
        this.dst = dst;
        this.batchType = dst.batchType();
        this.outCols = dst.vars().size();
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
     * the {@link Throwable}s are delivered via {@link CompletableBatchQueue#complete(Throwable)}
     * to the destination set on instantiation of the parser. IO errors and other errors not
     * related to parsing should be delivered to the parser via
     * {@link #feedError(FSException)}.</p>
     *
     * <p>This method is not thread-safe. Concurrent calls will silently lead to
     * data corruption.</p>
     *
     * @param rope non-null {@link Rope} with UTF-8 bytes representing a SPARQL results set.
     * @throws TerminatedException see {@link CompletableBatchQueue#offer(Batch)}
     * @throws CancelledException  see {@link CompletableBatchQueue#offer(Batch)}
     */
    public final void feedShared(SegmentRope rope) throws TerminatedException, CancelledException {
        try {
            if (rope == null || rope.len == 0)
                return; // no-op
            if ((boolean)TERMINATED.getAcquire(this))
                throw TerminatedException.INSTANCE;
            if (batch == null)
                batch = dst.fillingBatch();
            doFeedShared(rope);
            emitBatch();
        } catch (TerminatedException|CancelledException e) {
            emitLastBatch();
            if (!(boolean)TERMINATED.compareAndExchangeRelease(this, false, true))
                cleanup(e);
            throw e;
        } catch (Throwable t) {
            handleFeedSharedError(t);
        }
    }

    /**
     * Notifies the parser that the input serialization stream has ended without an IO error.
     *
     * <p>This will cause completion of the destination {@link CompletableBatchQueue} via
     * {@link CompletableBatchQueue#complete(Throwable)} an {@link InvalidSparqlResultsException}
     * may be delivered to the {@link CompletableBatchQueue} if the end-of-input was unexpected
     * given previous contents fed to {@link #feedShared(SegmentRope)}.</p>
     */
    public final void feedEnd() {
        if (!(boolean)TERMINATED.getAcquire(this)) {
            if (batch == null)
                batch = batchType.create(outCols);
            Throwable error = doFeedEnd();
            if (!(boolean)TERMINATED.compareAndExchangeRelease(this, false, true)) {
                emitLastBatch();
                beforeComplete(error);
                dst.complete(error);
                cleanup(null);
            }
        }
    }

    /**
     * This should be invoked by parser implementations once the results serialization
     * format itself (and not the underlying protocol, such as HTTP or WebSockets) contains
     * an acknowledgment that the results are incomplete due to a client-requested cancel
     * and that no more results follow.
     */
    protected final void feedCancelledAck() {
        if (!(boolean)TERMINATED.compareAndExchangeRelease(this, false, true)) {
            if (batch == null)
                batch = batchType.create(outCols);
            Throwable e = doFeedEnd();
            emitLastBatch();
            beforeComplete(e != null ? e : CancelledException.INSTANCE);
            if (e == null) dst.cancel(true);
            else           dst.complete(e);
            cleanup(null);
        }
    }

    /**
     * Complete the parser with the given error that did not arise from syntax or semantic errors
     * in the results serialization. This should be used to deliver network failures.
     * Cancellations should be directly delivered via {@link CompletableBatchQueue#cancel(boolean)}.
     *
     * @param error A non-null, non-serialization and non-cancellation error.
     */
    public final void feedError(FSException error) {
        if (!(boolean)TERMINATED.compareAndExchangeRelease(this, false, true)) {
            emitLastBatch();
            dst.complete(error);
            cleanup(error);
        }
    }

    /**
     * Reset the parser state, as if it had been just instantiated and received no
     * {@link #feedShared(SegmentRope)}/{@link #feedEnd()}/{@link #feedError(FSException)} call.
     *
     * <p>This should only be called if the destination is ready to receive another
     * {@link CompletableBatchQueue#complete(Throwable)} call.</p>
     */
    public void reset() {
        if (batch != null && batch.rows > 0) {
            if (ThreadJournal.ENABLED)
                journal("batch.totalRows=", batch.totalRows(), "during reset on", this);
            batch.clear();
        }
        incompleteRow   = false;
        eager           = false;
        rowsParsed      = 0;
        TERMINATED.setRelease(this, false);
    }

    /*  --- --- --- abstract methods --- --- --- */

    /**
     * Called once per {@link ResultsParser}, after parsing is complete or failed.
     *
     * @param cause The error forwarded to {@link CompletableBatchQueue#complete(Throwable)} or
     * {@link CancelledException#INSTANCE} if this is being called due to a
     * {@link CompletableBatchQueue#cancel(boolean)}ed destination.
     */
    protected void cleanup(@Nullable Throwable cause) {
        if (batch != null) {
            if (ThreadJournal.ENABLED && batch.rows > 0)
                journal("batch.totalRows=", batch.totalRows(), "during cleanup of", this);
            batch = dst.batchType().recycle(batch);
        }
    }

    /**
     * Implement the parsing as specified by {@link ResultsParser#feedShared(SegmentRope)}.
     *
     * <p>Exceptions thrown by this method will be wrapped as {@link InvalidSparqlResultsException}
     * and delivered downstream via {@link CompletableBatchQueue#complete(Throwable)}.</p>
     * @throws TerminatedException see {@link #commitRow()}
     * @throws CancelledException see {@link #commitRow()}
     */
    protected abstract void doFeedShared(SegmentRope rope) throws TerminatedException, CancelledException ;

    /**
     * This method will be called by a {@link #feedEnd()} that was not preceded by a
     * {@link #feedError(FSException)}.
     *
     * @return {@code null} if an end-of-input is legal given previous content, else an
     *         {@link InvalidSparqlResultsException} describing the error.
     */
    protected abstract @Nullable Throwable doFeedEnd();

    /**
     * This is called after the last batch has been delivered and before
     * {@link CompletableBatchQueue#complete(Throwable)} is called.
     *
     * @param error the same (possibly null) error that will be delivered to
     *              {@link CompletableBatchQueue#complete(Throwable)}
     */
    protected void beforeComplete(@Nullable Throwable error) {}

    /*  --- --- --- helpers --- --- --- */

    public Vars         vars()      { return dst.vars(); }
    public BatchType<B> batchType() { return dst.batchType(); }

    protected boolean isTerminated() { return (boolean)TERMINATED.getOpaque(this); }

    public long rowsParsed() { return rowsParsed; }

    protected void commitRow() throws CancelledException, TerminatedException {
        ++rowsParsed;
        incompleteRow = false;
        batch.commitPut();
        if (eager) {
            emitBatch();
            if (batch == null)
                batch = batchType.create(outCols);
        }
    }

    protected final void beginRow() {
        incompleteRow = true;
        batch.beginPut();
    }

    /*  --- --- --- private helpers --- --- --- */

    private void handleFeedSharedError(Throwable t) throws TerminatedException {
        if ((boolean)TERMINATED.compareAndExchangeRelease(this, false, true)) {
            log.info("{} already terminated, ignoring {}", this, t.getClass().getSimpleName(), t);
        } else {
            emitLastBatch();
            Throwable ex = t instanceof FSException e ? e : new InvalidSparqlResultsException(t);
            dst.complete(ex);
            cleanup(ex);
        }
        throw new RuntimeException(t);
    }

    private void emitLastBatch() {
        if (batch != null) {
            try {
                if (incompleteRow) { // drop incomplete row
                    incompleteRow = false;
                    batch.abortPut();
                }
                emitBatch();
            } catch (CancelledException|TerminatedException ignored) {
            } catch (Throwable t) {
                log.warn("Ignoring emitLastBatch() failure", t);
            }
        }
    }

    private void emitBatch() throws CancelledException, TerminatedException {
        B b;
        if (incompleteRow) {
            eager = true; // emit when row completes on next feedShared()
        } else if ((b = this.batch) != null && b.rows > 0) {
            batch = null;
            eager = false;
            if ((b = dst.offer(b)) != null)
                (batch = b).clear();
        }
    }
}
