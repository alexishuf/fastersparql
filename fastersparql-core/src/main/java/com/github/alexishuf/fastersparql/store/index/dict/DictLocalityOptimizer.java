package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.store.index.OffsetMappedLEValues;
import com.github.alexishuf.fastersparql.store.index.SmallBBPool;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.store.index.dict.Dict.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.SUFFIX;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.fromConcatChar;
import static java.lang.String.format;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

class DictLocalityOptimizer {
    private static final Logger log = LoggerFactory.getLogger(DictLocalityOptimizer.class);

    public static void convert(Path path) throws IOException {
        long stringsAndFlags = readStringsAndFlags(path);
        if ((stringsAndFlags & LOCALITY_MASK) != 0) {
            log.debug("Will not optimize {}: already has flag", path);
            return;
        }
        Path tempDest = path.getParent().resolve(path.getFileName() + ".tmp");
        log.info("Generating locality optimized version of {} at {}...", path, tempDest);
        try (Converter converter = new Converter(tempDest, path, stringsAndFlags)) {
            converter.convert();
        } catch (Throwable t) {
            try {
                Files.deleteIfExists(tempDest);
            } catch (IOException e) {
                log.warn("Ignoring failure to delete temp file {}: {}", tempDest, e.getMessage());
            }
            throw t;
        }
        Files.move(tempDest, path, REPLACE_EXISTING);
    }

    private static final class Reader extends OffsetMappedLEValues {
        public Reader(Path path, Arena scope) throws IOException {
            super(path, scope);
        }

        public MemorySegment seg() { return seg; }
        public int offShift() { return offShift; }
        public long offsCount() { return offsCount; }

        @Override public long readOff(long index) {
            return super.readOff(index);
        }

        @Override protected void fillMetadata(MemorySegment seg, Metadata md) {
            long stringsAndFlags = seg.get(JAVA_LONG, 0);
            md.valueWidth = 1;
            md.offsetsOff = Dict.OFFS_OFF;
            md.offsetWidth = (stringsAndFlags & Dict.OFF_W_MASK) == 0 ? 8 : 4;
            md.offsetsCount = (stringsAndFlags & STRINGS_MASK) + 1;
        }

    }

    private static class Converter implements AutoCloseable {
        private boolean closed = false;
        private final Reader reader;
        private final MemorySegment out;
        private final boolean embedSharedId;
        private final int offWidth;
        private final long nStrings;
        private long lastLog;

        public Converter(Path destPath, Path srcPath, long srcStringsAndFlags) throws IOException {
            if (srcPath.normalize().equals(destPath.normalize()))
                throw new IOException("src and dest paths are the same: "+srcPath);
            Arena arena = Arena.ofShared();
            reader = new Reader(srcPath, arena);
            embedSharedId = reader.seg().byteSize() < LocalityCompositeDict.OFF_MASK
                         && (srcStringsAndFlags & SHARED_MASK) != 0;
            offWidth = embedSharedId || (srcStringsAndFlags & OFF_W_MASK) == 0 ? 8 : 4;
            ByteBuffer bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN);
            try (var ch = FileChannel.open(destPath, WRITE, READ, TRUNCATE_EXISTING, CREATE)) {
                if (ch.write(bb.clear().put((byte)0).flip()) != 1) {
                    throw new IOException("Could not grow file to required size");
                }
                long inTableBytes = OFFS_OFF + (reader.offsCount() << reader.offShift());
                long u8Bytes = reader.seg().byteSize() - inTableBytes;
                if (embedSharedId)
                    u8Bytes -= 5*(reader.offsCount()-1);
                long outSize = OFFS_OFF + (reader.offsCount()*offWidth) + u8Bytes;
                out = ch.map(READ_WRITE, 0, outSize, arena);
            }
            out.set(LE_LONG, 0, srcStringsAndFlags
                    | LOCALITY_MASK | (embedSharedId ? OFF_W_MASK : 0));
            nStrings = srcStringsAndFlags & STRINGS_MASK;
        }

        @Override public void close() {
            if (closed) return;
            out.force();
            closed = true;
            reader.close(); // also closes arena
        }

        public void convert() {
            lastLog = Timestamp.nanoTime();
            writeSourceIdx(0, 1);
            copyStrings();
        }

        private long writeSourceIdx(long srcIdx, long outId) {
            if (outId <= nStrings) {
                srcIdx = writeSourceIdx(srcIdx, outId<<1);
                writeOff(outId, srcIdx++);
                srcIdx = writeSourceIdx(srcIdx, (outId<<1) + 1);
            }
            return srcIdx;
        }

        private void writeOff(long id, long value) {
            if (offWidth == 4) out.set(LE_INT,  OFFS_OFF + ((id - 1) << 2), (int) value);
            else               out.set(LE_LONG, OFFS_OFF + ((id - 1) << 3), value);
        }

        private long readOff(long id) {
            return offWidth == 4 ? out.get(LE_INT, OFFS_OFF + ((id - 1) << 2))
                                 : out.get(LE_INT, OFFS_OFF + ((id - 1) << 3));
        }

        private void copyStrings() {
            MemorySegment in = reader.seg();
            long u8Dest = OFFS_OFF + (nStrings+1)*offWidth;
            for (long id = 1; id <= nStrings; ++id) {
                if (Timestamp.nanoTime()-lastLog > 10_000_000_000L) {
                    lastLog = Timestamp.nanoTime();
                    log.info("Optimizing cache friendliness of {}: {}/{} strings ({}%)",
                             reader.path, id-1, nStrings, format("%.3f", 100.0*(id-1)/nStrings));
                }
                // read string off & len
                long srcIdx = readOff(id);
                long off = reader.readOff(srcIdx);
                int len = (int)(reader.readOff(srcIdx+1) - off);
                // handle embedSharedId
                long outOff = u8Dest;
                if (embedSharedId) {
                    int shId = Splitter.decode(in, off); // parse the shared id from the base64
                    if (fromConcatChar(in.get(JAVA_BYTE, off+4)) == SUFFIX)
                        shId |= LocalityCompositeDict.SH_ID_SUFF; // encode suffix operator !
                    off += 5; // do not copy base64 prefix
                    len -= 5;
                    outOff |= (long)shId << LocalityCompositeDict.SH_ID_BIT;
                }
                // copy string and write offset
                MemorySegment.copy(in, off, out, u8Dest, len);
                writeOff(id, outOff);
                u8Dest += len;
            }
            writeOff(nStrings+1, u8Dest);
        }
    }

    private static long readStringsAndFlags(Path path) throws IOException {
        ByteBuffer bb = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN);
        try (FileChannel ch = FileChannel.open(path, READ)) {
            if (ch.read(bb.limit(8)) != 8)
                throw new IOException("Could not read 8 bytes from "+path);
            return bb.getLong(0);
        } finally {
            SmallBBPool.releaseSmallDirectBB(bb);
        }
    }
}
