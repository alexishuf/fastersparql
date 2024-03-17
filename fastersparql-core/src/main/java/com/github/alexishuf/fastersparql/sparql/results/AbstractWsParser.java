package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractWsParser<B extends Batch<B>> extends SVParser.Tsv<B> {
    protected boolean serverSentTermination = false;

    /* --- --- --- vocabulary for the WebSocket protocol --- --- --- */

    public static final byte[] BIND_REQUEST     = "!bind-request ".getBytes(UTF_8);
    public static final byte[] BIND_EMPTY_UNTIL = "!bind-empty-streak ".getBytes(UTF_8);
    public static final byte[] PREFIX           = "!prefix ".getBytes(UTF_8);
    public static final byte[] INFO             = "!info ".getBytes(UTF_8);
    public static final byte[] PING             = "!ping\n".getBytes(UTF_8);
    public static final byte[] PING_ACK         = "!ping-ack\n".getBytes(UTF_8);
    public static final byte[] ERROR            = "!error".getBytes(UTF_8);
    public static final byte[] CANCEL           = "!cancel".getBytes(UTF_8);
    public static final byte[] CANCEL_LF        = "!cancel\n".getBytes(UTF_8);
    public static final byte[] CANCELLED        = "!cancelled".getBytes(UTF_8);
    public static final byte[] END              = "!end".getBytes(UTF_8);
    public static final byte[] END_LF           = "!end\n".getBytes(UTF_8);
    public static final byte[] REQUEST          = "!request ".getBytes(UTF_8);
    public static final byte[] MAX              = "MAX".getBytes(UTF_8);

    /* --- --- --- constructors --- --- --- */

    public AbstractWsParser(CompletableBatchQueue<B> dst) {
        super(dst);
    }

    /* --- --- --- abstract methods --- --- --- */

    /** The remote peer sent a !ping-ack message in response to a !ping frame. */
    protected void onPingAck() { /* pass */ }

    protected abstract void onPing();

    protected void onInfo(SegmentRope rope, int begin, int end) { /* pass */}

    /** The remote peer wants the processing to stop. It will not send any more input and
     *  any further input should be treated as an error. */
    protected abstract void onCancel();

    /** Handle a client/server-specific control message in rope.sub(begin, eol) and return true
     *  iff there is such message type. */
    protected abstract boolean handleRoleSpecificControl(Rope rope, int begin, int eol);

    /* --- --- --- implementations --- --- --- */

    @Override public void reset() {
        super.reset();
        serverSentTermination = false;
    }

    @Override protected final int handleControl(SegmentRope rope, int begin) {
        for (int end = rope.len(), eol; begin < end && rope.get(begin) == '!'; begin = eol+1) {
            if ((eol = rope.skipUntil(begin, end, '\n')) == end)
                return suspend(rope, begin, end);
            byte first = begin+1 < end ? rope.get(begin+1) : 0;
            if (first == 'e' && rope.has(begin, END))
                handleEnd(rope, eol);
            else if (first == 'e' && rope.has(begin, ERROR))
                handleError(rope, begin, eol);
            else if (first == 'c' && rope.has(begin, CANCELLED))
                handleCancelled();
            else if (first == 'c' && rope.has(begin, CANCEL))
                handleCancel();
            else if (first == 'p' && rope.has(begin, PREFIX))
                handlePrefix(rope, begin, eol);
            else if (first == 'p' && rope.has(begin, PING_ACK))
                onPingAck();
            else if (first == 'p' && rope.has(begin, PING))
                onPing();
            else if (first == 'i' && rope.has(begin, INFO))
                onInfo(rope, begin, eol);
            else if (!handleRoleSpecificControl(rope, begin, eol))
                throw badControl(rope, begin, eol);
            ++line;
        }
        return begin;
    }

    /* --- --- --- helper methods --- --- --- */

    private void handlePrefix(SegmentRope r, int begin, int eol) {
        int nameBegin = begin+PREFIX.length, colon = r.skipUntil(nameBegin, eol, ':');
        if (nameBegin >= eol || colon >= eol) throw badPrefix(r, begin, colon);

        Term iri = null;
        int iriBegin = colon+1;
        if (eol-iriBegin >= 2) {
            try {
                if (r.get(colon + 1) == '<')
                    iri = Term.valueOf(r, iriBegin, eol);
                else {
                    var wrapped = new ByteRope(eol - iriBegin + 2)
                            .append('<').append(r, colon + 1, eol).append('>');
                    iri = Term.splitAndWrap(wrapped);
                }
            } catch (InvalidTermException ignored) { }
        }
        if (iri == null)
            throw badPrefix(r, begin, eol);
        var name = new ByteRope(colon - nameBegin).append(r, nameBegin, colon);
        termParser.prefixMap.addRef(name, iri);
    }

    private void handleCancel() {
        serverSentTermination = true;
        onCancel();
    }

    private void handleError(Rope rope, int begin, int eol) {
        serverSentTermination = true;
        throw new FSServerException(rope.toString(begin+ERROR.length, eol));
    }

    private void handleCancelled() {
        serverSentTermination = true;
        feedCancelledAck();
    }

    private void handleEnd(Rope rope, int eol) {
        if (eol+1 != rope.len()) {
            var msg = format("Received input after !end at line %d. Buggy server " +
                            "or accidental sharing of WebSocket channel: %s", line,
                    rope.toString(eol + 1, rope.len()).replace("\r", "\\r").replace("\n", "\\n"));
            throw new InvalidSparqlResultsException(msg);
        }
        serverSentTermination = true;
        feedEnd();
    }

    private InvalidSparqlResultsException badPrefix(Rope rope, int begin, int colon) {
        int eol = rope.skipUntil(begin, rope.len(), '\n');
        int nameLen = colon-begin+(PREFIX.length+1);
        String reason = nameLen <= 0 ? "Missing prefix name"
                                     : colon >= eol ? "Missing :" : "Malformed !prefix";
        String msg = reason + " at line " + line +": "+ rope.toString(begin, eol);
        return new InvalidSparqlResultsException(msg);
    }

    private InvalidSparqlResultsException badControl(Rope rope, int begin, int eol) {
        var msg = format("Invalid WS results control command at line %d: %s",
                line, rope.sub(begin, eol));
        return new InvalidSparqlResultsException(msg);
    }

}
