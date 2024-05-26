package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.store.index.RopeHandlePool;
import com.github.alexishuf.fastersparql.store.index.SmallBBPool;
import com.github.alexishuf.fastersparql.store.index.Sorter;
import com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.store.index.dict.Dict.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.Mode.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.SUFFIX;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Arrays.fill;

public class DictSorter extends Sorter<Path> implements NTVisitor {
    private static final Logger log = LoggerFactory.getLogger(DictSorter.class);
    private static final boolean IS_DEBUG = log.isDebugEnabled();

    private final int bytesPerBlock;

    private boolean stringsClosed = false;
    public boolean sharedOverflow;
    private final boolean usesShared;
    private final boolean optimizeLocality;
    public Splitter.Mode split = Splitter.Mode.LAST;

    private @Nullable DictBlock fillingBlock;

    public DictSorter(Path tempDir, boolean usesShared, boolean optimizeLocality) {
        this(tempDir, usesShared, optimizeLocality, defaultBlockBytes());
    }

    public DictSorter(Path tempDir, boolean usesShared, boolean optimizeLocality, int bytesPerBlock) {
        super(tempDir, "dict", ".block");
        this.usesShared = usesShared;
        this.optimizeLocality = optimizeLocality;
        this.bytesPerBlock = bytesPerBlock;
        this.fillingBlock = new DictBlock(bytesPerBlock);
    }

    /**
     * Creates or overwrites {@code dest} with a dictionary containing all strings previously
     * given to {@link #copy(SegmentRope)}.
     */
    public void writeDict(Path dest) throws IOException {
        lock();
        try {
            stringsClosed = true;
            Path residual = runResidual(fillingBlock);
            fillingBlock = null;
            waitBlockJobs();
            if (residual != null)
                sorted.add(residual);
            if (sorted.isEmpty()) {
                try (DictBlock empty = new DictBlock(0)) { sorted.add(empty.run()); }
            }
            if (sorted.size() == 1) {
                Files.move(sorted.getFirst(), dest, REPLACE_EXISTING);
                var bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN).limit(8);
                try (var ch = FileChannel.open(dest, WRITE, READ)) {
                    if (ch.read(bb) != 8)
                        throw new IOException("Corrupted "+dest);
                    byte flags = (byte)(bb.get(7)
                            |  (usesShared           ? SHARED_MASK      >>> FLAGS_BIT : 0)
                            |  (sharedOverflow       ? SHARED_OVF_MASK  >>> FLAGS_BIT : 0)
                            |  (split == PROLONG     ? PROLONG_MASK     >>> FLAGS_BIT : 0)
                            |  (split == PENULTIMATE ? PENULTIMATE_MASK >>> FLAGS_BIT : 0)
                    );
                    bb.put(7, flags).flip();
                    if (ch.write(bb, 0) != 8)
                        throw new IOException("Refused to write all 8 header bytes");
                } finally {
                    SmallBBPool.releaseSmallDirectBB(bb);
                }
            } else {
                Comparator<SegmentRope> comparator = optimizeLocality && usesShared
                        ? LocalityRopeComparator.INSTANCE
                        : Comparator.naturalOrder();
                try (Merger m = new Merger(sorted, dest, usesShared, sharedOverflow,
                                           split, comparator)) {
                    m.write();
                }
            }
            if (optimizeLocality)
                DictLocalityOptimizer.convert(dest);
            if (IS_DEBUG)
                logStats(dest);
        } finally {
            unlock();
        }
    }

    private void logStats(Path dest) {
        ByteBuffer bb = null;
        long unique = 0, tableBytes = 0, fileBytes = dest.toFile().length();
        boolean usesShared = false, sharedOverflow = false;
        int offWidth = 0;
        try (var ch = FileChannel.open(dest, READ)) {
            bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN).limit(8);
            if (ch.read(bb) != 8)
                throw new IOException("Dict smaller than 8 bytes");
            long stringsAndFlags = bb.getLong(0);
            offWidth = (stringsAndFlags & OFF_W_MASK) == 0 ? 8 : 4;
            usesShared = (stringsAndFlags & SHARED_MASK) != 0;
            sharedOverflow = (stringsAndFlags & SHARED_OVF_MASK) != 0;
            unique = stringsAndFlags & ~FLAGS_MASK;
            tableBytes = (unique+1) * offWidth;
        } catch (IOException e) {
            log.error("Failed to read first bytes from {}: {}", dest, e.toString());
        } finally {
            SmallBBPool.releaseSmallDirectBB(bb);
        }
        long sum = 0;
        List<Path> sorted = this.sorted;
        if (sorted.size() == 1)
            sorted = List.of(dest);
        for (Path p : sorted) {
            bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN).limit(8);
            try (var ch = FileChannel.open(p, READ)) {
                if (ch.read(bb) != 8)
                    throw new IOException("Dict smaller than 8 bytes");
                sum += bb.getLong(0)&~FLAGS_MASK;
            } catch (IOException e) {
                log.error("Failed to open block at "+p);
            } finally {
                SmallBBPool.releaseSmallDirectBB(bb);
            }
        }
        log.debug("{}: {} blocks with {} KiB capacity summing {} strings ({} unique) " +
                  "offsets table cost: {}/{} ({}), offWidth={}, usesShared={}, sharedOverflow={}",
                  dest, sorted.size(), bytesPerBlock, sum, unique,
                  tableBytes, fileBytes, String.format("%.3f%%", 100.0*tableBytes/fileBytes),
                  offWidth, usesShared, sharedOverflow);
    }

    /**
     * Stores a copy of the given string for future dictionary generation.
     *
     * <p>This method may be called with strings out of order. They will be sorted when necessary.
     * This method does not keep any (indirect) reference to {@code string}. Thus the caller
     * may mutate it as soon as this method returns.</p>
     *
     * @param string a string to be stored.
     */
    public void copy(SegmentRope string) { copy(string, FinalSegmentRope.EMPTY); }

    @Override public void visit(SegmentRope string) { copy(string, FinalSegmentRope.EMPTY); }

    /**
     * Stores a copy of the string resulting from the concatenation of {@code prefix}
     * and {@code suffix}.
     *
     * <p>This method may be called with strings out of order. They will be sorted when necessary.
     * This method does not keep any (indirect) reference to {@code string}. Thus the caller
     * may mutate it as soon as this method returns.</p>
     *
     * @param prefix the prefix of the string to be stored
     * @param suffix the suffix of the string to be stored
     */
    public void copy(SegmentRope prefix, SegmentRope suffix) {
        lock();
        try {
            if (stringsClosed)
                throw new IllegalArgumentException("writeDict()/close() called");
            if (fillingBlock == null || !fillingBlock.copy(prefix, suffix)) {
                if (fillingBlock != null)
                    scheduleBlockJob(fillingBlock);
                if ((fillingBlock = (DictBlock) recycled()) == null)
                    fillingBlock = new DictBlock(bytesPerBlock);
                if (!fillingBlock.copy(prefix, suffix))
                    throw new IllegalArgumentException("string too large");
            }
        } finally {
            unlock();
        }
    }

    /* --- --- --- internals --- --- --- */

    private static final class Merger implements SafeCloseable {
        private final SortedStandaloneDict[] blockDicts;
        private final SortedStandaloneDict.Lookup[] blocks;
        private final SegmentRope[] currStrings;
        private final long[] currIds;
        private final FileChannel destChannel;
        private final boolean usesShared, sharedOverflow;
        private final Splitter.Mode split;
        private final Comparator<SegmentRope> comparator;
        private int currBlock;

        public Merger(List<Path> blockFiles, Path dest, boolean usesShared,
                      boolean sharedOverflow, Splitter.Mode split,
                      Comparator<SegmentRope> comparator) throws IOException {
            this.usesShared = usesShared;
            this.sharedOverflow = sharedOverflow;
            this.split = split;
            this.comparator = comparator;
            int n = blockFiles.size();
            blockDicts = new SortedStandaloneDict[n];
            blocks = new SortedStandaloneDict.Lookup[n];
            FileChannel destChannel = null;
            var condenser = new ExceptionCondenser<>(IOException.class, IOException::new);
            try {
                for (int i = 0; i < n; i++) {
                    var dict = new SortedStandaloneDict(blockFiles.get(i));
                    blockDicts[i] = dict;
                    blocks[i] = dict.lookup().takeOwnership(this);
                }
                log.info("Validating block files: {}", blockFiles);
                List<Path> invalid = IntStream.range(0, n).parallel()
                        .mapToObj(i -> invalidBlock(blockFiles, i))
                        .filter(Objects::nonNull).toList();
                if (!invalid.isEmpty())
                    throw new IOException("Invalid blocks: "+invalid);
                destChannel = FileChannel.open(dest, TRUNCATE_EXISTING,CREATE,WRITE);
            } catch (Throwable t) {
                condenser.condense(t);
                ExceptionCondenser.closeAll(Arrays.stream(blocks).map(AbstractLookup::dict).iterator());
            }
            try {
                condenser.throwIf();
            } catch (Throwable t) {
                if (destChannel != null) destChannel.close();
                throw t;
            }
            this.destChannel = destChannel;
            // init string pointers pointing to "before first"
            currIds = new long[n];
            currStrings = new SegmentRope[n];
            fill(currIds, MIN_ID-1);
        }

        @Nullable private Path invalidBlock(List<Path> blockFiles, int i) {
            Path path = blockFiles.get(i);
            var lookup = blocks[i];
            @SuppressWarnings("resource") long strings = lookup.dict().strings();
            try (var last = PooledSegmentRopeView.ofEmpty()) {
                SegmentRope curr = lookup.get(1);
                if (curr == null) {
                    if (strings == 0) return null;
                    log.error("null string in non-empty block {}", path);
                    return path;
                }
                last.wrap(curr);
                for (int id = 2; id <= strings; id++) {
                    if ((curr = lookup.get(id)) == null) {
                        log.error("null string at id={} of {}", id, path);
                        return path;
                    }
                    int diff = comparator.compare(curr, last);
                    if (diff > 0) {
                        last.wrap(curr);
                    } else if (diff == 0) {
                        log.error("duplicate string at id={} of {}", id, path);
                        return path;
                    } else {
                        log.error("smaller string at id={} of {}", id, path);
                        return path;
                    }
                }
            }
            return null;
        }

        @Override public void close() {
            ExceptionCondenser.closeAll(Arrays.asList(blockDicts).iterator());
            for (int i = 0; i < blocks.length; i++)
                blocks[i] = Owned.safeRecycle(blocks[i], this);
        }

        /** Merges the content of all sorted blocks writing then to the destination dict file. */
        public void write() throws IOException {
            resetIteration();
            long strings = 0, bytes = 0, visited = 0, last = Timestamp.nanoTime();
            for (SegmentRope s; (s = nextString()) != null; ++strings) bytes += s.len;

            resetIteration();
            try (var w = new DictWriter(destChannel, strings, bytes,
                                        usesShared, sharedOverflow, split)) {
                for (SegmentRope s; (s = nextString()) != null; ) {
                    w.writeSorted(s);
                    ++visited;
                    if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                        log.info("Wrote {}/{} strings ({}%)", visited, strings,
                                 String.format("%.3f", 100.0*visited/strings));
                        last = Timestamp.nanoTime();
                    }
                }
            }
        }

        /** Move block 0 to "before first" and all others to "first" */
        private void resetIteration() {
            currBlock = 0;
            fill(currIds, Dict.MIN_ID-1);
            for (int i = 1; i < blocks.length; i++)
                advance(i); // advance from "before first" to "first"
        }

        /** Advance to next id for the {@code block}-th block. Will return null if reached end. */
        private @Nullable SegmentRope advance(int block) {
            SegmentRope s = blocks[block].get(++currIds[block]);
            currStrings[block] = s;
            return s;
        }

        /** Advances the id of the current block and get the next lowest non-duplicate string  */
        private SegmentRope nextString() {
            // advance position on currBlock, we may exhaust it with this
            advance(currBlock);

            // find the smallest string while skipping over inter-block duplicates
            SegmentRope min = null;
            currBlock = -1;
            blocks:
            for (int i = 0; i < blocks.length; i++) {
                SegmentRope candidate = currStrings[i];
                while (true) {
                    if (candidate == null) continue blocks; // block ended
                    if (min == null) break;                 // no min, make candidate the min
                    int diff = comparator.compare(candidate, min);
                    if (diff < 0) break;                    // candidate < min
                    if (diff > 0 || (candidate = advance(i)) == null)
                        continue blocks; // candidate > min or block only had a duplicate
                }
                min = candidate;
                currBlock = i;
            }
            return min;
        }
    }

    private static final class LocalityRopeComparator implements Comparator<SegmentRope> {
        public static final LocalityRopeComparator INSTANCE = new LocalityRopeComparator();

        @Override public int compare(SegmentRope l, SegmentRope r) {
            MemorySegment lSeg = l.segment(), rSeg = r.segment();
            long lOff = l.offset(), rOff = r.offset();
            int lFlaggedId = Splitter.decode(lSeg, lOff);
            int rFlaggedId = Splitter.decode(rSeg, rOff);
            if (SharedSide.fromConcatChar(l.get(4)) == SUFFIX)
                lFlaggedId |= LocalityCompositeDict.SH_ID_SUFF;
            if (SharedSide.fromConcatChar(r.get(4)) == SUFFIX)
                rFlaggedId |= LocalityCompositeDict.SH_ID_SUFF;
            int diff = lFlaggedId - rFlaggedId;
            if (diff != 0) return diff;
            return SegmentRope.compare1_1(lSeg, lOff+5, l.len-5,
                                         rSeg, rOff+5, r.len-5);
        }
    }

    private final class DictBlock implements BlockJob<Path> {
        private final Arena arena;
        private final MemorySegment bytes;
        private SegmentRopeView[] ropes;
        private final TwoSegmentRope offer = new TwoSegmentRope();
        private int nBytes = 0, nRopes = 0;

        public DictBlock(int bytesCapacity) { this(bytesCapacity, Math.max(2, bytesCapacity>>>10)); }
        public DictBlock(int bytesCapacity, int ropesCapacity) {
            // native memory here will avoid one copy when rope[i] is written to a FileChannel
            arena = Arena.ofShared();
            bytes = arena.allocate(bytesCapacity, 8);
            ropes = new SegmentRopeView[ropesCapacity];
        }

        @Override public void      reset() { nBytes = nRopes = 0; }
        @Override public boolean isEmpty() { return nRopes == 0; }

        @Override public void close() {
            arena.close();
            for (int i = 0, n = nRopes; i < n; i++) {
                var r = ropes[i];
                if (r != null) {
                    if (RopeHandlePool.offer(r) == null)
                        ropes[i] = null;
                    else
                        break;
                }
            }
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean copy(SegmentRope prefix, SegmentRope suffix) {
            // check if string was recently added (5 covers current and last triple)
            offer.wrapFirst(prefix);
            offer.wrapSecond(suffix);
            for (int i = Math.max(0, nRopes-5), e = nRopes-1; i < e; i++) {
                //noinspection EqualsBetweenInconvertibleTypes
                if (offer.equals(ropes[i])) return true;
            } // else: prefix+suffix might be novel

            int prefixLen = prefix.len, suffixLen = suffix.len;
            int len = prefixLen + suffixLen, nBytes = this.nBytes;

            // check for space in bytes
            if (nBytes + len > bytes.byteSize())
                return false; // no space

            // store a new rope backed by this.bytes, growing ropes if necessary
            if (nRopes == ropes.length)
                ropes = Arrays.copyOf(ropes, ropes.length + (ropes.length>>1));
            var handle = ropes[nRopes];
            if (handle == null) ropes[nRopes] = handle = RopeHandlePool.segmentRope();
            handle.wrap(bytes, null, nBytes, len);

            //store UTF-8 bytes
            MemorySegment.copy(prefix.segment(), prefix.offset(), bytes, nBytes, prefixLen);
            MemorySegment.copy(suffix.segment(), suffix.offset(),
                               bytes, nBytes+prefixLen, suffixLen);

            // commit the addition
            ++nRopes;
            this.nBytes = nBytes+len;
            return true;
        }

        public Path run() throws IOException {
            Path dest = createTempFile();
            int nRopes = this.nRopes, end = this.nRopes;
            // sort, then remove duplicates by writing nulls
            if (optimizeLocality && usesShared) {
                Arrays.sort(ropes, 0, end, LocalityRopeComparator.INSTANCE);
            } else {
                Arrays.sort(ropes, 0, end);
            }
            SegmentRope last = null;
            for (int i = 0; i < end; i++) {
                var r = ropes[i];
                if (r.equals(last)) { ropes[i] = null; --nRopes; }
                else                last = r;
            }
            //write all ropes
            try (var ch = FileChannel.open(dest, WRITE,CREATE,TRUNCATE_EXISTING);
                 var w = new DictWriter(ch, nRopes, nBytes, false, false, LAST)) {
                w.writeSorted(ropes, 0, end);
            }
            return dest;
        }
    }
}
