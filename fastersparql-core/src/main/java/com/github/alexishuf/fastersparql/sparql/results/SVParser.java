package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.BN_PREFIX_u8;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.UNTIL_LIT_ESCAPED;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class SVParser<B extends Batch<B>> extends ResultsParser<B> {
    private static final Logger log = LoggerFactory.getLogger(SVParser.class);

    protected int nVars;
    protected final FinalSegmentRope eol;
    protected TermParser termParser;
    protected @Nullable MutableRope partialLine, fedPartialLine;
    protected int inputColumns = -1, column, line;
    protected int[] inVar2outVar;

    private SVParser(FinalSegmentRope eol, CompletableBatchQueue<B> destination) {
        super(destination);
        this.nVars = destination.vars().size();
        this.eol = eol;
        this.termParser = TermParser.create().takeOwnership(this);
    }

    @Override public void reset(CompletableBatchQueue<B> downstream) {
        super.reset(downstream);
        if (termParser == null)
            termParser = TermParser.create().takeOwnership(this);
        inputColumns = -1;
        column       =  0;
        line         =  0;
        nVars        = downstream.vars().size();
        if (partialLine    != null) partialLine   .clear();
        if (fedPartialLine != null) fedPartialLine.clear();
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        termParser = termParser.recycle(this);
        if (partialLine != null)
            partialLine.close();
        if (fedPartialLine != null)
            fedPartialLine.close();
    }

    @Override protected @Nullable Throwable doFeedEnd() {
        try {
            if (partialLine != null && partialLine.len > 0) {
                boolean hasEOL = findEOL(partialLine, 0, partialLine.len) < partialLine.len;
                handlePartialLine(hasEOL ? FinalSegmentRope.EMPTY : eol);
                if (partialLine != null && partialLine.len > 0) //
                    return unclosedQuote();
            } else if (column > 0) {
                try {
                    commitRow();
                } catch (CancelledException | TerminatedException ignored) {
                }
            }
        } catch (FSException e) {
            return e;
        } catch (Throwable t) {
            return new InvalidSparqlResultsException(t);
        }
        return null;
    }

    public final static class TsvFactory implements ResultsParser.Factory {
        @Override public SparqlResultFormat name() { return SparqlResultFormat.TSV; }
        @Override public <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> d) {
            return new Tsv<>(d);
        }
    }

    public final static class CsvFactory implements ResultsParser.Factory {
        @Override public SparqlResultFormat name() { return SparqlResultFormat.CSV; }
        @Override
        public <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> d) {
            return new Csv<>(d);
        }
    }

    public static class Tsv<B extends Batch<B>> extends SVParser<B> {
        private static final FinalSegmentRope EOL = FinalSegmentRope.asFinal("\n");

        public Tsv(CompletableBatchQueue<B> destination) { super(EOL, destination); }

        @Override public SparqlResultFormat format() {return SparqlResultFormat.TSV;}

        @Override protected final void doFeedShared(SegmentRope rope) throws CancelledException, TerminatedException {
            int begin = 0, end = rope.len();
            if (partialLine != null && partialLine.len > 0) {
                handlePartialLine(rope);
                return;
            }
            if (inputColumns == -1) {
                if ((begin = handleControl(rope, begin)) < end)
                    begin = readVars(rope, begin, end, '\t');
            }
            if (nVars == 0)
                begin = parseAsk(rope, begin, end);
            int lastCol = Math.max(0, inputColumns-1);
            for (byte c = 0; begin < end; ++begin) {
                while (begin != end && ((c = rope.get(begin)) == ' ' || c == '\r')) ++begin;
                if (column == 0 && c == '!')
                    c = (byte)((begin = handleControl(rope, begin)) < end ? rope.get(begin) : 0);
                if (begin >= end)
                    break;
                int pseudTermLast = switch (c) {
                    case '<' -> rope.skipUntil(begin, end, '>');
                    case '"' -> rope.skipUntilUnescaped(begin+1, end, '"');
                    default  -> begin;
                };
                if (rope.skipUntil(pseudTermLast, end, column == lastCol ? '\n' : '\t') == end) {
                    suspend(rope, begin, end);
                    return;
                } else if (c != '\t' && c != '\n') { //only parse if column is not empty
                    switch (termParser.parse(rope, begin, end)) {
                        case NT, TTL   -> setTerm();
                        case VAR       -> throw varAsValue(termParser.asTerm());
                        case MALFORMED -> throw badTerm(rope, begin, termParser.explain());
                        case EOF       -> { suspend(rope, begin, end); return; }
                    }
                    begin = termParser.termEnd();
                }
                while (begin != end && ((c = rope.get(begin)) == ' ' || c == '\r')) ++begin;
                c = begin < end ? rope.get(begin) : 0;
                if (c == '\t') {
                    if (column++ >= lastCol) throw extraColumns();
                } else if (c == '\n') {
                    if (column   != lastCol) throw missingColumns();
                    column = 0;
                    ++line;
                    if (!incompleteRow)
                        beginRow();
                    commitRow();
                } else if (c != 0) {
                    throw badSep(c);
                }
            }
        }

        @Override protected int findEOL(Rope rope, int begin, int end) {
            while (begin < end && (begin = rope.skipUntil(begin, end, '"', '\n')) < end
                               && rope.get(begin) == '"')
                begin = rope.skipUntilUnescaped(begin, end, '"')+1;
            return Math.min(begin, end);
        }


        private int parseAsk(SegmentRope rope, int begin, int end) throws CancelledException, TerminatedException {
            if (begin < end && rope.get(begin) == '!')
                begin = handleControl(rope, begin);
            if (findEOL(rope, begin, end) >= end)
                return suspend(rope, begin, end);
            byte c = rope.get(begin);
            boolean positive = true;
            if (c != '\t' && c != '\n') {
                positive = switch (termParser.parse(rope, begin, end)) {
                    case NT, TTL   -> !termParser.asTerm().equals(Term.FALSE);
                    case EOF       -> true;
                    case VAR       -> throw varAsValue(termParser.asTerm());
                    case MALFORMED -> throw badTerm(rope, begin, termParser.explain());
                };
            }
            ++line;
            if (positive) {
                beginRow();
                commitRow();
            }
            return end;
        }

        protected int handleControl(SegmentRope rope, int begin) { return begin; }
    }

    public final static class Csv<B extends Batch<B>> extends SVParser<B> {
        private static final FinalSegmentRope EOL = FinalSegmentRope.asFinal("\r\n");
        private final SegmentRopeView view = new SegmentRopeView();
        private final MutableRope escaped = new MutableRope(64);

        public Csv(CompletableBatchQueue<B> destination) {
            super(EOL, destination);
            termParser.eager();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            view.wrapEmpty();
            escaped.close();
        }

        @Override public SparqlResultFormat format() {return SparqlResultFormat.CSV;}

        @Override protected void doFeedShared(SegmentRope rope) throws CancelledException, TerminatedException {
            if (partialLine != null && partialLine.len != 0) {
                handlePartialLine(rope);
                return;
            }
            int begin = 0, end = rope.len();
            if (line == 0)
                begin = readVars(rope, begin, end, ',');
            if (nVars == 0)
                begin = parseAsk(rope, begin, end);
            while (begin < end) {
                begin = parseCsv(rope, begin, end);
                byte c = begin < end ? rope.get(begin) : 0;
                if (c == ',') {
                    ++column;
                    if (column >= inputColumns) throw extraColumns();
                    ++begin;
                } else if (c == '\r') {
                    if (begin+1 >= end) {
                        suspend(rope, begin, end);
                        break;
                    } else if (rope.get(begin+1) == '\n') {
                        if (column != Math.max(0, inputColumns-1)) throw missingColumns();
                        column = 0;
                        ++line;
                        begin += eol.len;
                        if (!incompleteRow)
                            beginRow();
                        commitRow();
                    } else {
                        throw badSep(rope.get(begin+1));
                    }
                } else if (c != 0) {
                    throw badSep(c);
                }
            }
        }

        /** Find the first {@code i} in {@code [b,e)} where {@code r} has "," or "\r\n". */
        private int skipUntilCsvSep(Rope r, int b, int e) {
            while ((b = r.skipUntil(b, e, ',', '\r')) < e && r.get(b) != ',') {
                if (b+1 < e && r.get(b+1) == '\n') break;
                ++b;
            }
            return b;
        }

        private static final byte[][] IRI_SCHEMES = {
                "http://".getBytes(UTF_8),
                "https://".getBytes(UTF_8),
                "ftp://".getBytes(UTF_8),
                "ftps://".getBytes(UTF_8),
                "mailto:".getBytes(UTF_8),
                "urn:".getBytes(UTF_8)
        };

        /**
         * Try to parse a Term from a CSV column starting at {@code begin} in {@code rope} reading up
         * to index {@code end}. A term ends when the parser meets a ',' or a "\r\n" sequence outside
         * a "-quoted segment (where {@code "} itself is used as the escape char (see RFC 4180)).
         *
         * <p>The Term (if found) will be passed to {@code builder.set(column, term)}. If there is no
         *    complete term, {@code rope.sub(begin,end)} will be appended to {@code partialTerm}.</p>
         *
         * @return The new value for {@code begin}, which may be {@code end} or the index of a column
         * or line separator.
         */
        private int parseCsv(SegmentRope rope, int begin, int end) {
            byte first = 0;
            while (begin != end && ((first = rope.get(begin)) == ' ' || first == '\t')) ++begin;
            int lexBegin = first == '"' ? begin+1 : begin;
            int lexEnd   = first == '"' ? skipUntilUnescapedQuote(rope, lexBegin, end)
                                        : skipUntilCsvSep(rope, begin, end);
            int lexLen = lexEnd-lexBegin;
            if (lexEnd >= end)
                return suspend(rope, begin, end);
            if (lexEnd > lexBegin) {
                SegmentRope nt;
                if (lexBegin+1 < lexEnd && rope.has(lexBegin, BN_PREFIX_u8)) {
                    nt = view.wrap(rope, lexBegin, lexLen);
                } else {
                    escaped.clear().append('"').ensureFreeCapacity(lexLen+8);
                    for (int i = lexBegin, j; i < lexEnd; i = j + 1) {
                        escaped.append(rope, i, j = rope.skip(i, lexEnd, UNTIL_LIT_ESCAPED));
                        switch (j == lexEnd ? 0 : rope.get(j)) {
                            case '"' -> {
                                escaped.append('\\').append('"');
                                ++j;
                            }
                            case '\n' -> escaped.append('\\').append('n');
                            case '\r' -> escaped.append('\\').append('r');
                            case '\\' -> escaped.append('\\').append('\\');
                        }
                    }
                    boolean iri = false;
                    for (int i = 0; !iri && i < IRI_SCHEMES.length; i++)
                        iri = escaped.has(1, IRI_SCHEMES[i]);
                    if (iri) escaped.append('>').u8()[0] = '<';
                    else escaped.append('"');
                    nt = escaped;
                }
                switch (termParser.parse(nt, 0, nt.len)) {
                    case NT, TTL -> setTerm();
                    default -> throw badTerm(rope, begin, termParser.explain());
                }
            }
            begin = first == '"' ? lexEnd+1 : lexEnd;
            while (begin != end && ((first = rope.get(begin)) == ' ' || first == '\t')) ++begin;
            return begin;
        }

        private static final byte[] FALSE = "false".getBytes(UTF_8);
        private int parseAsk(Rope rope, int begin, int end) throws CancelledException, TerminatedException {
            if (findEOL(rope, begin, end) >= end)
                return suspend(rope, begin, end);
            if (line > 1) {
                if (line == 2)
                    log.debug("Ignoring unexpected rows in ASK query result");
                return end;
            }
            byte first = 0;
            while (begin != end && ((first = rope.get(begin)) == ' ' || first == '\t')) ++begin;
            int lexBegin = first == '"' ? begin+1 : begin;
            int lexEnd   = first == '"' ? skipUntilUnescapedQuote(rope, lexBegin, end)
                                        : skipUntilCsvSep(rope, begin, end);
            boolean positive = switch (lexEnd-lexBegin) {
                case 1 -> rope.get(lexBegin) != '0';
                case 5 -> !rope.has(lexBegin, FALSE);
                default -> true;
            };
            ++line;
            if (positive) {
                beginRow();
                commitRow();
            }
            feedEnd();
            return end;
        }

        @Override protected int findEOL(Rope rope, int begin, int end) {
            boolean quoted = false;
            while (begin < end && (begin = rope.skipUntil(begin, end, '"', '\r')) < end) {
                byte follow = begin+1 < end ? rope.get(begin+1) : 0;
                if (rope.get(begin) == '"') {
                    if (follow == '"') ++begin;
                    else               quoted = !quoted;
                } else if (follow == '\n' && !quoted) {
                    break;
                }
                ++begin;
            }
            return begin;
        }
    }

    protected int readVars(Rope rope, int begin, int end, char sep) {
        int eol = findEOL(rope, begin, end);
        if (eol >= end)
            return suspend(rope, begin, end);
        var offer = new Vars.Mutable(10);
        for (int termEnd, next; begin < eol; begin = next) {
            byte c = rope.get(begin = rope.skipWS(begin, eol));
            if (c == '"') {// quoted var name, termEnd is at closing quote
                begin = rope.skipWS(++begin, eol); // remove left-padding within quotes
                termEnd = skipUntilUnescapedQuote(rope, begin, eol);
                if (termEnd == eol)
                    throw new InvalidSparqlResultsException("Unclosed \" at line 0");
                //next term starts after first sep after "
                next = rope.skipUntil(termEnd+1, eol, sep)+1;
            } else {// unquoted. go to sep or eol
                termEnd = rope.skipUntil(begin, eol, sep);
                next = termEnd + 1; // next term starts after sep
            }
            termEnd = rope.rightTrim(begin, termEnd);

            if ((c = rope.get(begin)) == '?' || c == '$')
                ++begin; // do not include var marker into var name
            var varName = asFinal(rope, begin, termEnd);
            if (rope.skip(begin, termEnd, SparqlSkip.VARNAME) != termEnd)
                throw new InvalidSparqlResultsException("Invalid var name: "+varName);
            offer.add(varName);
        }
        inputColumns = offer.size();
        inVar2outVar = ArrayAlloc.intsAtLeast(inputColumns, inVar2outVar);
        if (nVars == 0 && offer.size() > (offer.contains(WsBindingSeq.VAR) ? 1 : 0))
            checkAskVars(offer);
        Vars vars = vars();
        for (int i = 0; i < offer.size(); i++)
            inVar2outVar[i] = vars.indexOf(offer.get(i));
        ++line;
        return eol+this.eol.len; // return index of first not consumed byte
    }

    protected void handlePartialLine(Rope rope) throws CancelledException, TerminatedException {
        if (partialLine == null)
            partialLine = new MutableRope(rope.len).append(rope);
        else
            partialLine.append(rope);

        if (findEOL(partialLine, 0, partialLine.len) < partialLine.len) {
            MutableRope tmp = partialLine;
            if ((partialLine = fedPartialLine) != null)
                partialLine.clear();
            doFeedShared(fedPartialLine = tmp);
        }
    }

    protected int skipUntilUnescapedQuote(Rope rope, int begin, int end) {
        if (eol.len == 1)
            return rope.skipUntilUnescaped(begin, end, '"');
        while ((begin = rope.skipUntil(begin, end, '"')) < end
                && begin+1 < end && rope.get(begin+1) == '"')
            begin += 2;
        return begin;
    }

    protected abstract int findEOL(Rope rope, int begin, int end);

    protected int suspend(Rope rope, int begin, int end) {
        if (end > begin) {
            if (partialLine == null)
                partialLine = new MutableRope(32 + (end - begin));
            partialLine.append(rope, begin, end);
        }
        return end;
    }

    protected void setTerm() {
        int dest = inVar2outVar[column];
        if (dest >= 0) {
            if (!incompleteRow)
                beginRow();
            batch.putTerm(dest, termParser);
        }
    }

    protected InvalidSparqlResultsException varAsValue(Term var) {
        var msg = String.format("Var %s given as value to column %d of line %d",
                         var, column, line);
        return new InvalidSparqlResultsException(msg);
    }

    protected InvalidSparqlResultsException unclosedQuote() {
        assert partialLine != null;
        int i = partialLine.len;
        byte esc = (byte) (eol.len == 1 ? '\\' : '"');
        while ((i = partialLine.reverseSkipUntil(0, i, '"')) > 0
                    && partialLine.get(i-1) != esc)
            --i;
        String msg = String.format("Unclosed quoted term at column %d of line %d.", column, line);
        return new InvalidSparqlResultsException(msg);
    }

    protected InvalidSparqlResultsException badTerm(Rope rope, int begin, Throwable t) {
        char eoc = eol == Tsv.EOL ? '\t' : ',';
        int termEnd = rope.skipUntil(begin, rope.len(), eoc, '\n');
        String actual = rope.sub(begin, termEnd).toString().replace("\r", "\\r")
                            .replace("\n", "\\n").replace("\t", "\\t").replace("\\", "\\\\");
        Vars vars = vars();
        var msg = String.format("Bad value starting at column %d (%s) of line %d. Cause: %s. Input: %s",
                         column, column < vars.size() ? vars.get(column) : "no var",
                         line, t.getMessage(), actual);
        return new InvalidSparqlResultsException(msg);
    }

    protected InvalidSparqlResultsException missingColumns() {
        var msg = String.format("Line %d ended with %d columns, expected %d", line, column, inputColumns);
        return new InvalidSparqlResultsException(msg);
    }

    private static final Pattern ASK_VAR = Pattern.compile("(?i)_*(ask)?[_.-]*(Result|Answer|Value)?s?_*");
    protected void checkAskVars(Vars actual) {
        String expected = "ASK results parser expected no vars or a single var for a boolean value. ";
        String msg = null;
        if (actual.size() > 1) {
            msg = expected +  "Got " + actual;
        } else if (actual.size() == 1 && !ASK_VAR.matcher(actual.getFirst().toString()).matches()) {
            msg = expected + actual.getFirst() + " does not appear to be an ask query result var";
        }
        if (msg != null)
            throw  new InvalidSparqlResultsException(msg);
    }

    protected InvalidSparqlResultsException extraColumns() {
        var msg = String.format("More than %d columns at line %d. Expected %s", inputColumns, line,
                eol == Csv.EOL ? "\\r\\n (\\x0D\\x0A, CRLF)" : "\\n(\\x0A, LF)");
        return new InvalidSparqlResultsException(msg);
    }

    protected InvalidSparqlResultsException badSep(byte actual) {
        String msg = String.format("Expected %s, got '%s' (0x%x) at line %d",
                column >= inputColumns - 1 ? eol.len == 1 ? "\"\\n\"" : "\"\\r\\n\""
                                    : eol.len == 1 ? "'\\t'" : "','",
                (char)(0xff & actual),
                0xff & actual,
                line
        );
        return new InvalidSparqlResultsException(msg);
    }
}

