package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.util.BS;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.Files.getLastModifiedTime;
import static java.nio.file.StandardOpenOption.*;

public class LexicalPresence {
    private static final long COOKIE = 0x016c61636978656cL; // "lexical\u0001"
    private static final int DEF_ROWS = 1<<16;
    private static final Logger log = LoggerFactory.getLogger(LexicalPresence.class);

    public static final int            NO_TYPE      = -2;
    public static final int  BEFORE_FIRST_TYPE      = -1;
    public static final int         BLANK_TYPE      = 0;
    public static final int           IRI_TYPE      = 1;
    public static final int         PLAIN_TYPE      = 2;
    public static final int       LIT_SUF_TYPE_BASE = 3;

    private static final ValueLayout.OfInt   LE_INT = JAVA_INT .withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = JAVA_LONG.withOrder(LITTLE_ENDIAN);
    private final long[] has;
    private final int rowsMask;
    private final int typesCount;
    private final FinalSegmentRope[] litSuf;

    private LexicalPresence(int rows, int typesCount, long[] has, FinalSegmentRope[] litSuf) {
        if (Integer.bitCount(rows) != 1)
            throw new IllegalArgumentException("Number of rows is not a power of 2");
        if (rows*typesCount > has.length<<6)
            throw new IllegalArgumentException("More addressable bits than contained in bitset");
        this.rowsMask   = rows-1;
        this.typesCount = typesCount;
        this.has        = has;
        this.litSuf     = litSuf;
    }

    public static @Nullable LexicalPresence load(Path dictFile, Path lexicalFile,
                                                 FinalSegmentRope[] litSuf) throws IOException {
        if (!Files.isRegularFile(lexicalFile))
            return null; // not lexical file
        if (getLastModifiedTime(lexicalFile).compareTo(getLastModifiedTime(dictFile)) < 0) {
            log.info("{} is older than {}, refusing to load", lexicalFile,
                    dictFile.relativize(lexicalFile));
            return null; // strings is newer than lexical
        }
        try (var ch = FileChannel.open(lexicalFile, StandardOpenOption.READ);
             var arena = Arena.ofConfined()) {
            var seg     = ch.map(MapMode.READ_ONLY, 0, ch.size(), arena);
            long cookie = seg.get(LE_LONG, 0);
            if (cookie != COOKIE) {
                log.info("Bad cookie={} for {}, will not load",
                         Long.toHexString(cookie), lexicalFile);
                return null;
            }
            int rows       = seg.get(LE_INT, 8);
            int typesCount = seg.get(LE_INT, 12);
            var has        = new long[BS.longsFor(rows*typesCount)];
            MemorySegment.copy(seg, LE_LONG, 16, has, 0, has.length);
            if (LIT_SUF_TYPE_BASE + litSuf.length > typesCount) {
                log.info("typesCount in {} does not match litSuf.length. Will not load",
                         lexicalFile);
                return null;
            }
            return new LexicalPresence(rows, typesCount, has, litSuf);
        } catch (IndexOutOfBoundsException e) {
            log.error("Corrupt {}: will not load", lexicalFile);
            return null;
        } catch (Throwable t) {
            log.error("Failed to load {}: {}", lexicalFile, t.toString());
            throw t;
        }
    }

    public void write(Path file) throws IOException {
        try (var ch = FileChannel.open(file, CREATE,READ,WRITE,TRUNCATE_EXISTING);
             var arena = Arena.ofConfined()) {
            int bytes = 16 + has.length*8;
            ch.truncate(bytes);
            var seg = ch.map(MapMode.READ_WRITE, 0, bytes, arena);
            seg.set(LE_LONG, 0, COOKIE);
            seg.set(LE_INT, 8, rowsMask+1);
            seg.set(LE_INT, 12, typesCount);
            MemorySegment.copy(has, 0, seg, LE_LONG, 16, has.length);
            seg.force();
            ch.force(true);
        } catch (Throwable t) {
            log.error("Failed to write to {}: {}", file, t.toString());
            throw t;
        }
    }

    public static Builder build(FinalSegmentRope[] litSuffixes) {
        return new Builder(litSuffixes);
    }

    public static final class Builder {
        private final int typesCount;
        private final FinalSegmentRope[] litSuf;
        private final long[] has;
        private final int rowsMask = DEF_ROWS-1;

        private Builder(FinalSegmentRope[] litSuf) {
            this.litSuf     = litSuf;
            this.typesCount = LIT_SUF_TYPE_BASE+litSuf.length;
            this.has        = new long[BS.longsFor(DEF_ROWS*(LIT_SUF_TYPE_BASE+litSuf.length))];
        }

        public void add(TwoSegmentRope nt) {
            long slice = lexSlice(nt);
            int type = sliceType(slice);
            if (type == LIT_SUF_TYPE_BASE) {
                int end = sliceEnd(slice);
                for (int i = 0; i < litSuf.length; i++) {
                    if (nt.has(end, litSuf[i])) {
                        type = LIT_SUF_TYPE_BASE+i;
                        break;
                    }
                }
            }
            BS.set(has, row(nt, slice, rowsMask)*typesCount + type);
        }

        public LexicalPresence build() {
            return new LexicalPresence(rowsMask+1, typesCount, has, litSuf);
        }
    }

    private static int sliceType (long slice) { return (int)(slice>>>60); }
    private static int sliceBegin(long slice) { return (int)(slice>>>32)&0x0fffffff; }
    private static int sliceEnd  (long slice) { return (int) slice; }

    private static long lexSlice(PlainRope nt) {
        int ntLen = nt.len;
        if (ntLen < 2)
            return 0;
        int beginLex, endLex;
        int type = switch (nt.get(0)) {
            case '"' -> {
                beginLex = 1;
                if (nt.get(ntLen-1) == '"') {
                    endLex = ntLen-1;
                    yield PLAIN_TYPE;
                } else {
                    endLex = nt.skipUntilLastFar(0, ntLen, (byte)'"');
                    yield LIT_SUF_TYPE_BASE;
                }
            }
            case '<' -> {
                beginLex = 1;
                endLex   = ntLen-1;
                yield IRI_TYPE;
            }
            case '_' -> {
                if (ntLen < 3) {
                    beginLex = endLex = 0;
                } else {
                    endLex   = ntLen;
                    beginLex = 2;
                }
                yield BLANK_TYPE;
            }
            default ->
                throw new IllegalArgumentException("Invalid N-Triples");
        };
        return ((long)type<<60) | ((long)beginLex<<32) | (endLex&0xffffffffL);
    }

    private static int row(PlainRope nt, long slice, int rowsMask) {
        return nt.fastHash(sliceBegin(slice), sliceEnd(slice))&rowsMask;
    }

    public int typesCount() { return typesCount; }

    /**
     * Get a value for use with {@link #findNextType(int, int)}.
     * @param nt the N-Triples representation of an RDF term
     */
    public  int baseOf(PlainRope nt) { return baseOf(nt, lexSlice(nt)); }
    private int baseOf(PlainRope nt, long slice) { return row(nt, slice, rowsMask)*typesCount; }

    /**
     * Get the lexical alternative type (the {@code int} that would be returned by
     * {@link #findNextType(int, int)}) of {@code nt}.
     *
     * <p>The result of this method should be used to skip evaluating a type identified by
     * {@link #findNextType(int, int)} in case lexical joins are evaluated after
     * a traditional join.</p>
     *
     * @param nt An RDF term in N-Triples syntax.
     * @return Either {@link #BLANK_TYPE}, {@link #IRI_TYPE}, {@link #PLAIN_TYPE} or
     *         {@link #LIT_SUF_TYPE_BASE}+{@code indexOfUniqueLitSuffix}.
     */
    @SuppressWarnings("unused")
    public  int typeOf(PlainRope nt) { return  typeOf(nt, lexSlice(nt)); }
    private int typeOf(PlainRope nt, long slice) {
        int type = sliceType(slice), end = sliceEnd(slice);
        if (type == LIT_SUF_TYPE_BASE) {
            type = -1;
            for (int i = 0, ntLen = nt.len; i < litSuf.length; i++) {
                var suf = litSuf[i];
                if (end+suf.len == ntLen &  nt.has(end, suf)) {
                    type = LIT_SUF_TYPE_BASE+i;
                    break;
                }
            }
        }
        return type;
    }

    /**
     * Equivalent to {@code ((long)baseOf(nt, slice)<<32) | (typeOf(nt, slice)&0xffffffffL)}.
     *
     * @see #baseOf(PlainRope)
     * @see #typeOf(PlainRope)
     */
    @SuppressWarnings("unused")
    public long baseOfAndType(PlainRope nt) {
        long slice = lexSlice(nt);
        return ((long)baseOf(nt, slice)<<32) | (typeOf(nt, slice)&0xffffffffL);
    }

    /**
     * Finds the next type for which the lexical form that originated {@code base}
     * <strong>MAY</strong> have items in the dictionary. Notice this presence test is,
     * by design, prone to false positives: it will return {@code true} when there are no
     * instances. However, a return of {@code false} is always correct.
     *
     * @param base a value obtained from {@link #baseOf(PlainRope)}
     * @param lastType The last return of this method for {@code base} or
     *                 {@link #BEFORE_FIRST_TYPE} if this is the first call for {@code base}
     * @return if there is no more types, {@link #NO_TYPE}, else {@link #BLANK_TYPE},
     *         {@link #IRI_TYPE}, {@link #PLAIN_TYPE} or
     *         {@link #LIT_SUF_TYPE_BASE}+{@code indexOfUniqueLitSuffix}
     */
    public int findNextType(int base, int lastType) {
        int i = BS.nextSetOrEnd(has, base+lastType+1, base+typesCount)-base;
        return i < typesCount ? i : NO_TYPE;
    }
}
