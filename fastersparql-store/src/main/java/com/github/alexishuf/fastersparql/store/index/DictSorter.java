package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
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
import java.util.List;

import static com.github.alexishuf.fastersparql.store.index.Dict.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class DictSorter extends Sorter<Path> {
    private static final Logger log = LoggerFactory.getLogger(DictSorter.class);
    private static final boolean IS_DEBUG = log.isDebugEnabled();

    private final int bytesPerBlock;

    private boolean stringsClosed = false;
    public boolean usesShared, sharedOverflow;

    private @Nullable DictBlock fillingBlock;

    public DictSorter(Path tempDir) {
        this(tempDir, 128*1024*1024);
    }

    public DictSorter(Path tempDir, int bytesPerBlock) {
        super(tempDir, "dict", ".block");
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
            if (sorted.size() == 0) {
                try (DictBlock empty = new DictBlock(0)) { sorted.add(empty.run()); }
            }
            if (sorted.size() == 1) {
                Files.move(sorted.get(0), dest, REPLACE_EXISTING);
                var bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN).limit(8);
                try (var ch = FileChannel.open(dest, WRITE, READ)) {
                    if (ch.read(bb) != 8)
                        throw new IOException("Corrupted "+dest);
                    byte flags = (byte)(bb.get(7)
                            |  (usesShared     ? SHARED_MASK    >>>FLAGS_BIT : 0)
                            |  (sharedOverflow ? SHARED_OVF_MASK>>>FLAGS_BIT : 0));
                    bb.put(7, flags).flip();
                    if (ch.write(bb, 0) != 8)
                        throw new IOException("Refused to write all 8 header bytes");
                } finally {
                    SmallBBPool.releaseSmallDirectBB(bb);
                }
            } else {
                try (Merger m = new Merger(sorted, dest, usesShared, sharedOverflow)) {
                    m.write();
                }
            }
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
    public void copy(SegmentRope string) {
        copy(string, ByteRope.EMPTY);
    }

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

    private static final class Merger implements AutoCloseable {
        private final Dict[] blocks;
        private final long[] currentStringIds;
        private final SegmentRope[] currentStrings;
        private final FileChannel destChannel;
        private final boolean usesShared, sharedOverflow;
        private final SegmentRope lastString = RopeHandlePool.segmentRope();

        public Merger(List<Path> blockFiles, Path dest,
                      boolean usesShared, boolean sharedOverflow) {
            this.usesShared = usesShared;
            this.sharedOverflow = sharedOverflow;
            int n = blockFiles.size();
            blocks = new Dict[n];
            FileChannel destChannel = null;
            try {
                for (int i = 0; i < n; i++)//noinspection resource
                    blocks[i] = new Dict(blockFiles.get(i));
                destChannel = FileChannel.open(dest, TRUNCATE_EXISTING,CREATE,WRITE);
            } catch (Throwable t) {
                ExceptionCondenser.closeAll(Arrays.stream(blocks).iterator());
            }
            this.destChannel = destChannel;
            currentStringIds = new long[n];
            currentStrings   = new SegmentRope[n<<1];
            currentStringIds[0] = Dict.MIN_ID-1;
            currentStrings  [0] = RopeHandlePool.segmentRope();
            for (int i = 1; i < n; i++) {
                var s = RopeHandlePool.segmentRope();
                currentStrings[i] = s;
                blocks[i].get(currentStringIds[i] = MIN_ID, s);
            }
        }

        @Override public void close() {
            ExceptionCondenser.closeAll(Arrays.stream(blocks).iterator());
            for (int i = 0; i < currentStrings.length; i++) {
                SegmentRope r = currentStrings[i];
                if (r == null) continue;
                if (RopeHandlePool.offer(r) != null) break;
                currentStrings[i] = null;
            }
            RopeHandlePool.offer(lastString);
        }

        /** Merges the content of all sorted blocks writing then to the destination dict file. */
        public void write() throws IOException {
            long strings = 0, bytes = 0, visited = 0, last = Timestamp.nanoTime();
            for (int i = 0; (i = nextBlock(i)) >= 0; ++strings)
                bytes += currentStrings[i].len;
            // reset iteration
            currentStringIds[0] = Dict.MIN_ID-1;
            currentStrings[0] = currentStrings[blocks.length];
            for (int i = 1; i < blocks.length; i++) {
                SegmentRope rope = currentStrings[blocks.length + i];
                currentStrings[i] = rope;
                if (blocks[i].get(1, rope)) {
                    currentStringIds[i] = 1;
                } else {
                    currentStrings[blocks.length+i] = currentStrings[i];
                    currentStrings[i] = null;
                }
            }
            try (var w = new DictWriter(destChannel, strings, bytes, usesShared, sharedOverflow)) {
                for (int i = 0; (i = nextBlock(i)) >= 0; ) {
                    w.writeSorted(currentStrings[i]);
                    ++visited;
                    if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                        log.info("Visited {}/{} strings", visited, strings);
                        last = Timestamp.nanoTime();
                    }
                }
            }
        }

        /** Get the current string for block or null if exhausted. Will advance if duplicate. */
        private @Nullable SegmentRope currString(int block) {
            var s = currentStrings[block];
            // skip cross-block duplicate string occurrences
            while (s != null && s.equals(lastString)) {
                if (!blocks[block].get(++currentStringIds[block], s)) { // exhausted
                    currentStrings[blocks.length+block] = s;
                    currentStrings[block] = s = null;
                }
            }
            return s;
        }

        /** Advances the id of the current block and finds the block with lowest, novel string */
        private int nextBlock(int currBlock) {
            //remember current string to remove duplicates
            lastString.wrap(currentStrings[currBlock]);

            // advance position on currBlock, we may exhaust it with this
            if (!blocks[currBlock].get(++currentStringIds[currBlock], currentStrings[currBlock])) {
                currentStrings[blocks.length+currBlock] = currentStrings[currBlock];
                currentStrings[currBlock] = null;
            }

            // find lowest string that is not lastString
            SegmentRope min = null;
            currBlock = -1;
            for (int i = 0; i < blocks.length; i++) {
                var candidate = currString(i); // internally advances block id if duplicate
                if (candidate != null && (min == null || candidate.compareTo(min) < 0)) {
                    min = candidate;
                    currBlock = i;
                }
            }
            return currBlock;
        }
    }

    private final class DictBlock implements BlockJob<Path> {
        private final Arena arena;
        private final MemorySegment bytes;
        private SegmentRope[] ropes;
        private final TwoSegmentRope offer = new TwoSegmentRope();
        private int nBytes = 0, nRopes = 0;

        public DictBlock(int bytesCapacity) { this(bytesCapacity, Math.max(2, bytesCapacity>>>10)); }
        public DictBlock(int bytesCapacity, int ropesCapacity) {
            // native memory here will avoid one copy when rope[i] is written to a FileChannel
            arena = Arena.openShared();
            bytes = arena.allocate(bytesCapacity, 8);
            ropes = new SegmentRope[ropesCapacity];
        }

        @Override public void      reset() { nBytes = nRopes = 0; }
        @Override public boolean isEmpty() { return nRopes == 0; }

        @Override public void close() {
            arena.close();
            for (int i = 0, n = nRopes; i < n; i++) {
                SegmentRope r = ropes[i];
                if (r != null && RopeHandlePool.offer(r) != null) break;
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
            handle.wrapSegment(bytes, nBytes, len);

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
            Arrays.sort(ropes, 0, end);
            SegmentRope last = null;
            for (int i = 0; i < end; i++) {
                var r = ropes[i];
                if (r.equals(last)) { ropes[i] = null; --nRopes; }
                else                last = r;
            }
            //write all ropes
            try (var ch = FileChannel.open(dest, WRITE,CREATE,TRUNCATE_EXISTING);
                 var w = new DictWriter(ch, nRopes, nBytes, false, false)) {
                w.writeSorted(ropes, 0, end);
            }
            return dest;
        }
    }
}
