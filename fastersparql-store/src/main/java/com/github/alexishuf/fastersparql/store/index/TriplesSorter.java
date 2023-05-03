package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.github.alexishuf.fastersparql.store.index.Triples.*;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.*;
import static java.util.concurrent.ForkJoinPool.commonPool;

public class TriplesSorter extends Sorter<TriplesBlock> {
    private static final Logger log = LoggerFactory.getLogger(TriplesSorter.class);

    private final int blockTriplesCapacity;
    private final Arena arena;
    private TriplesBlock filling;
    private boolean longIds = false;

    public TriplesSorter(Path tempDir) {
        this(tempDir, defaultBlockBytes()/24);
    }

    public TriplesSorter(Path tempDir, int blockTriplesCapacity) {
        super(tempDir, "triples", ".block");
        this.blockTriplesCapacity = blockTriplesCapacity;
        this.arena = Arena.openShared();
    }

    @Override public void close() {
        super.close();
        arena.close();
    }

    /**
     * Add a triple to the index.
     *
     * <p><strong>WARNING:</strong> duplicate triples are not supported.</p>
     *
     * @param s id of the triple subject
     * @param p id of the triple predicate
     * @param o id of the object predicate
     * @throws IllegalArgumentException if any of the ids is {@code <= 0}.
     */
    public void addTriple(long s, long p, long o) throws IOException {
        if (s <= 0 || p <= 0 || o <= 0)
            throw new IllegalArgumentException("Zero or negative id");
        if (!longIds && (s > Integer.MAX_VALUE || p > Integer.MAX_VALUE || o > Integer.MAX_VALUE))
            longIds = true;
        if (filling != null) {
            if (filling.add(s, p, o)) return;
            scheduleBlockJob(filling);
        }
        filling = new TriplesBlock(createTempFile(), arena.scope(), blockTriplesCapacity);
        if (!filling.add(s, p, o))
            throw new IOException("triple unexpectedly rejected");
    }

    /**
     * Writes the triple indices for the PSO, SPO, and OPS permutations using the previously
     * added triples.
     *
     * @param destDir destination directory where the triple indices will be created
     */
    public void write(Path destDir) throws IOException {
        File dirFile = destDir.toFile();
        if (!dirFile.isDirectory()) {
            if (dirFile.exists()) throw new IOException(destDir+" is not a dir");
            if (!dirFile.mkdirs()) throw new IOException("Could not mkdir "+destDir);
        }
        Path psoP = destDir.resolve("pso"), spoP = destDir.resolve("spo");
        Path opsP = destDir.resolve("ops");
        lock();
        try (var spo = FileChannel.open(spoP, CREATE, TRUNCATE_EXISTING, WRITE);
             var pso = FileChannel.open(psoP, CREATE, TRUNCATE_EXISTING, WRITE);
             var ops = FileChannel.open(opsP, CREATE, TRUNCATE_EXISTING, WRITE)) {
            filling = runResidual(filling);
            waitBlockJobs();
            if (filling != null && !filling.isEmpty())
                sorted.add(filling);
            filling = null;
            if (sorted.isEmpty())
                sorted.add(new TriplesBlock(createTempFile(), arena.scope(), 0));
            long triples = 0;
            for (TriplesBlock b : sorted) triples += b.triples;
            log.debug("{}: {} blocks with {} KiB capacity summing {} entries longIds={}",
                      destDir, sorted.size(), (blockTriplesCapacity *24L)>>10, triples, longIds);
//            Future<?> spoValidation, psoValidation;
            try (Merger merger = new Merger(sorted, longIds)) {
                merger.write(spo);
//                spoValidation = commonPool().submit(() -> validate(spoP));
                log.info("{}: shuffling spo -> pso...", destDir);
                shuffleAndSort(TriplesBlock::spo2pso);
                merger.write(pso);
//                psoValidation = commonPool().submit(() -> validate(spoP));
                log.info("{}: shuffling pso -> ops...", destDir);
                shuffleAndSort(TriplesBlock::pso2ops);
                merger.write(ops);
//                validate(opsP);
//                try {
//                    spoValidation.get();
//                    psoValidation.get();
//                } catch (InterruptedException | ExecutionException e) {
//                    throw new IOException(e);
//                }
            }
        } finally {
            unlock();
        }
    }

    private static final class Merger implements AutoCloseable {
        private static final int COUNT_KEYS = 0;
        private static final int WRITE_IDS = 1;

        private final TriplesBlock[] blocks;
        private final int[] positions;
        private final long triples;
        private final ByteBuffer tmp;
        private @MonotonicNonNull FileChannel ch;
        private final int idW;
        private int offW;
        private boolean bufferedPairs;
        private long firstKeyId, keys, lastKey, nextKeyOffDest, pairsDest;
        private long visited, lastLog;

        public Merger(List<TriplesBlock> blocks, boolean longIds) {
            this.blocks = blocks.toArray(TriplesBlock[]::new);
            this.positions = new int[this.blocks.length];
            long triples = 0;
            for (TriplesBlock b : this.blocks) triples += b.triples;
            this.triples = triples;
            this.idW = longIds ? 8 : 4;
            this.tmp = SmallBBPool.chunkDirectBB().order(LITTLE_ENDIAN);
        }

        @Override public void close() {
            SmallBBPool.releaseChunkDirectBB(tmp);
        }

        private void visit(int pass, long s, long p, long o) throws IOException {
            ++visited;
            if (Timestamp.nanoTime()-lastLog > 10_000_000_000L) {
                log.info("Pass {} visited {}/{}", pass, visited, triples);
                lastLog = Timestamp.nanoTime();
            }
            long lastKey = this.lastKey;
            if (pass == COUNT_KEYS) {
                if (s != lastKey) {
                    if (lastKey == 0) {
                        firstKeyId = s;
                        keys = 1;
                    } else {
                        keys += s - lastKey;
                    }
                    this.lastKey = s;
                }
                return;
            } else if (pass != WRITE_IDS) {
                throw new IllegalStateException();
            }
            // write Triples index
            if ((s != lastKey && bufferedPairs) || tmp.remaining() < idW*2)
                flushPairs();
            for (; lastKey < s; lastKey++) {
                write(nextKeyOffDest, putOff(pairsDest));
                nextKeyOffDest += offW;
            }
            this.lastKey = s;
            // buffer pair
            if (idW == 4) tmp.putInt((int)p).putInt((int)o);
            else          tmp.putLong(p).putLong(o);
            bufferedPairs = true;
        }

        private void flushPairs() throws IOException {
            pairsDest += write(pairsDest, tmp);
            bufferedPairs = false;
        }

        private void beginVisit(int pass) {
            visited = 0;
            lastLog = Timestamp.nanoTime();
            if (pass == COUNT_KEYS) {
                keys = 0;
                lastKey = 0;
            }
        }

        private void endVisit(int pass) throws IOException {
            if (pass == WRITE_IDS && bufferedPairs)
                flushPairs();
        }

        private void visit(int pass) throws IOException {
            Arrays.fill(positions, 0);
            positions[0] = -1;
            int bIdx = 0;
            beginVisit(pass);
            while (true) {
                if (++positions[bIdx] >= blocks[bIdx].triples) positions[bIdx] = -1;
                long s = Long.MAX_VALUE, p = Long.MAX_VALUE, o = Long.MAX_VALUE;
                bIdx = -1;
                for (int i = 0; i < blocks.length; i++) {
                    int tIdx = positions[i];
                    if (tIdx < 0) continue;
                    TriplesBlock b = blocks[i];
                    if (b.cmp(tIdx, s, p, o) < 0) {
                        long addr = tIdx * 24L;
                        bIdx = i;
                        s = b.seg.get(JAVA_LONG, addr);
                        p = b.seg.get(JAVA_LONG, addr + 8);
                        o = b.seg.get(JAVA_LONG, addr + 16);
                    }
                }
                if (bIdx >= 0) visit(pass, s, p, o);
                else           break;
            }
            endVisit(pass);
        }

        private ByteBuffer putOff(long value) {
            tmp.putLong(value);
            tmp.position(tmp.position()-(8-offW));
            return tmp;
        }

        private int write(long pos, ByteBuffer buf) throws IOException {
            int ex = buf.flip().remaining();
            if (ch.write(buf, pos) != ex)
                throw new IOException("FileChannel did not write all bytes");
            tmp.clear();
            return ex;
        }

        public void write(FileChannel ch) throws IOException {
            visit(COUNT_KEYS);
            if ((keys & ~Triples.NKEYS_MASK) != 0)
                throw new IllegalArgumentException("Too many keys");
            long estTotal = OFFS_OFF + (keys + 1) * 8 + triples * 2 * idW;
            offW = estTotal > Integer.MAX_VALUE ? 8 : 4;
            nextKeyOffDest = OFFS_OFF;
            pairsDest = OFFS_OFF + (keys+1)*offW;
            pairsDest += 8-(pairsDest&7);
            try (ch) {
                this.ch = ch;
                tmp.clear().putLong(keys | (idW == 4 ? ID_W_MASK : 0)
                                         | (offW == 4 ? OFF_W_MASK : 0))
                           .putLong(firstKeyId);
                // write size, flags and firstKeyId
                write(0, tmp);
                // write end offset for last key
                write(OFFS_OFF+keys*offW, putOff(pairsDest+triples*idW*2));
                // fill padding 4 bytes. if there is no padding this will be overwritten
                write(OFFS_OFF+(keys+1)*offW, tmp.putInt(0));
                lastKey = firstKeyId-1;
                visit(WRITE_IDS);
                ch.force(true);
            }
        }
    }

    private void shuffleAndSort(Consumer<TriplesBlock> shuffler) {
        if (sorted.size() == 1) {
            var b = sorted.get(0);
            shuffler.accept(b);
            b.sort(0, b.triples);
        } else {
            ArrayList<Callable<Void>> tasks = new ArrayList<>(sorted.size());
            for (TriplesBlock b : sorted)
                tasks.add(() -> {
                    shuffler.accept(b);
                    b.sort(0, b.triples);
                    return null;
                });
            try {
                for (Future<Void> f : commonPool().invokeAll(tasks))
                    f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
