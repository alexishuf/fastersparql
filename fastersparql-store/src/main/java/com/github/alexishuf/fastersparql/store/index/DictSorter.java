package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.alexishuf.fastersparql.util.concurrent.Async.*;
import static java.lang.System.identityHashCode;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class DictSorter implements AutoCloseable {
    private static final VarHandle WRITING;
    static {
        try {
            WRITING = lookup().findVarHandle(DictSorter.class, "plainWriting", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DictSorter.class);
    private final int bytesPerBlock;

    private final Path tempDir;
    private final List<Path> storedBlocks = new ArrayList<>();
    @SuppressWarnings("unused") private volatile int plainWriting;
    private boolean stringsClosed = false;

    private final ExceptionCondenser<IOException> blockJobError = new ExceptionCondenser<>(IOException.class, IOException::new);
    private final AtomicReference<Block> recycledBlock = new AtomicReference<>();
    private Block fillingBlock;
    private final ArrayBlockingQueue<BlockJob> blockJobs = new ArrayBlockingQueue<>(1);
    private final Thread blockJobsWorker;

    public DictSorter(Path tempDir) {
        this(tempDir, 128*1024*1024);
    }

    public DictSorter(Path tempDir, int bytesPerBlock) {
        this.bytesPerBlock = bytesPerBlock;
        this.fillingBlock = new Block(bytesPerBlock);
        this.tempDir = tempDir;
        this.blockJobsWorker = Thread.ofPlatform().name(toString()).start(this::blockJobsWorker);
    }

    @Override public void close() {
        checkConcurrency();
        try {
            // wait for block store thread
            while (blockJobsWorker.isAlive() && !blockJobs.offer(END_BLOCK_JOB))
                blockJobs.poll();
            uninterruptibleJoin(blockJobsWorker);
            // delete block files
            var errs = new ExceptionCondenser<>(IOException.class, IOException::new);
            for (Path file : storedBlocks) {
                try {
                    if (file.toFile().exists())
                        Files.delete(file);
                } catch (Throwable t) {
                    log.info("Failed to delete {}", file);
                    errs.condense(t);
                }
            }
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    private void checkConcurrency() {
        if ((int)WRITING.compareAndExchangeAcquire(this, 0, 1) != 0)
            throw new IllegalStateException("Concurrent close()/storeCopy()/writeDict()");
    }

    @Override public String toString() {
        return String.format("DictSorter@%x(%s)", identityHashCode(this), tempDir);
    }

    /**
     * Creates or overwrites {@code dest} with a dictionary containing all strings previously
     * given to {@link #storeCopy(SegmentRope)}.
     */
    public void writeDict(Path dest) throws IOException {
        checkConcurrency();
        try {
            stringsClosed = true;
            if (blockJobsWorker.isAlive()) {
                uninterruptiblePut(blockJobs, END_BLOCK_JOB);
                uninterruptibleJoin(blockJobsWorker);
            }
            var err = blockJobError.get();
            if (err != null)
                throw new IOException("Could not store intermediary block", err);
            if (storedBlocks.size() == 1 && fillingBlock.isEmpty()) {
                Files.move(storedBlocks.get(0), dest, REPLACE_EXISTING);
            } else if (storedBlocks.isEmpty()) {
                fillingBlock.store(dest);
            } else {
                if (!fillingBlock.isEmpty()) {
                    storeToTemp(fillingBlock);
                    fillingBlock = Block.EMPTY;
                }
                try (Merger m = new Merger(storedBlocks, dest)) {
                    m.write();
                }
            }
        } finally {
            WRITING.setRelease(this, 0);
        }
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
    public void storeCopy(SegmentRope string) {
        checkConcurrency();
        try {
            if (stringsClosed)
                throw new IllegalArgumentException("writeDict()/close() called");
            if (!fillingBlock.copy(string)) {
                uninterruptiblePut(blockJobs, fillingBlock);
                fillingBlock = recycledBlock.getAndSet(null);
                if (fillingBlock == null) fillingBlock = new Block(bytesPerBlock);
                else                      fillingBlock.reset();
                if (!fillingBlock.copy(string))
                    throw new IllegalArgumentException("string too large");
            }
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    /* --- --- --- internals --- --- --- */

    private static final class Merger implements AutoCloseable {
        private final Dict[] blocks;
        private final long[] currentStringIds;
        private final SegmentRope[] currentStrings;
        private final FileChannel destChannel;

        public Merger(List<Path> blockFiles, Path dest) {
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
            currentStrings  [0] = new SegmentRope();
            for (int i = 1; i < n; i++)
                currentStrings[i] = blocks[i].get(currentStringIds[i] = Dict.MIN_ID);
        }

        @Override public void close() {
            ExceptionCondenser.closeAll(Arrays.stream(blocks).iterator());
        }

        public void write() throws IOException {
            long strings = 0, bytes = 0;
            for (int i = 0; (i = nextBlock(i)) >= 0; ++strings)
                bytes += currentStrings[i].len;
            // reset iteration
            currentStringIds[0] = Dict.MIN_ID-1;
            currentStrings[0] = currentStrings[blocks.length];
            for (int i = 1; i < blocks.length; i++) {
                SegmentRope rope = currentStrings[blocks.length + i];
                currentStrings[i] = rope;
                blocks[i].get(1, rope);
                currentStringIds[i] = 1;
            }
            try (var w = new DictWriter(destChannel, strings, bytes)) {
                for (int i = 0; (i = nextBlock(i)) >= 0; )
                    w.writeSorted(currentStrings[i]);
            }
        }

        private int nextBlock(int currBlock) {
            if (!blocks[currBlock].get(++currentStringIds[currBlock], currentStrings[currBlock])) {
                currentStrings[blocks.length+currBlock] = currentStrings[currBlock];
                currentStrings[currBlock] = null;
            }
            SegmentRope min = null;
            currBlock = -1;
            for (int i = 0; i < currentStringIds.length; i++) {
                var candidate = currentStrings[i];
                if (candidate != null && (min == null || candidate.compareTo(min) < 0)) {
                    min = candidate;
                    currBlock = i;
                }
            }
            return currBlock;
        }
    }

    private interface BlockJob {
        default boolean isEnd() { return false; }
    }

    private static final class EndBlockJob implements BlockJob {
        @Override public boolean isEnd() { return true; }
    }
    private static final EndBlockJob END_BLOCK_JOB = new EndBlockJob();

    private static final class Block implements BlockJob {
        public static final Block EMPTY = new Block(0, 0);

        private final MemorySegment bytes;
        private SegmentRope[] ropes;
        private int nBytes = 0, nRopes = 0;

        public Block(int bytesCapacity) { this(bytesCapacity, Math.max(2, bytesCapacity>>>10)); }
        public Block(int bytesCapacity, int ropesCapacity) {
            // native memory here will avoid one copy when rope[i] is written to a FileChannel
            bytes = MemorySegment.allocateNative(bytesCapacity, SegmentScope.auto());
            ropes = new SegmentRope[ropesCapacity];
        }

        public void      reset() { nBytes = nRopes = 0; }
        public boolean isEmpty() { return nRopes == 0; }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean copy(SegmentRope rope) {
            if (rope == null) return true;
            int len = rope.len, nBytes = this.nBytes;
            // quick check for duplicates
            for (int i = Math.max(0, nRopes-5); i < nRopes; i++) {
                if (ropes[i].equals(rope)) return true; // do not store duplicate
            }

            // check for space in bytes
            if (nBytes + len > bytes.byteSize())
                return false; // no space

            // store a new rope backed by this.bytes, growing ropes if necessary
            if (nRopes == ropes.length)
                ropes = Arrays.copyOf(ropes, ropes.length + (ropes.length>>1));
            SegmentRope handle = ropes[nRopes];
            if (handle != null) handle.wrapSegment(bytes, nBytes, len);
            else                ropes[nRopes] = new SegmentRope(bytes, nBytes, len);
            ++nRopes;

            //store UTF-8 bytes
            MemorySegment.copy(rope.segment(), rope.offset(), bytes, nBytes, len);
            this.nBytes = nBytes+len;
            return true;
        }

        public void store(Path dest) throws IOException {
            Arrays.sort(ropes, 0, nRopes);
            try (var ch = FileChannel.open(dest, WRITE,CREATE,TRUNCATE_EXISTING);
                 var w = new DictWriter(ch, nRopes, nBytes)) {
                w.writeSorted(ropes, nRopes);
            }
        }
    }

    private void storeToTemp(Block block) throws IOException {
        Path file = Files.createTempFile(tempDir, "dict", ".block");
        block.store(file);
        storedBlocks.add(file);
    }

    private void blockJobsWorker() {
        boolean failed = false;
        for (BlockJob j; !(j = uninterruptibleTake(blockJobs)).isEnd(); ) {
            Block block = (Block) j;
            try {
                if (!failed)
                    storeToTemp(block);
            } catch (Throwable t) {
                blockJobError.condense(t);
                failed = true;
            } finally {
                recycledBlock.compareAndSet(null, block);
            }
        }
    }
}
