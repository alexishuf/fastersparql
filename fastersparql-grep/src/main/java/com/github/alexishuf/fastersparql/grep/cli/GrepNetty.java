package com.github.alexishuf.fastersparql.grep.cli;

import com.github.alexishuf.fastersparql.grep.AppException;
import com.github.alexishuf.fastersparql.grep.FileChunk;
import com.github.alexishuf.fastersparql.grep.FileOutputChunk;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.foreign.MemorySegment.copy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;

@Command(name = "netty", aliases = {"n"},
        description = "Extract incoming/outgoing log of a single channel from " +
                      "a NettyChannelDebugger dump")
public class GrepNetty implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(GrepNetty.class);
    private static final byte[] BEGIN_CH_MARKER = "[id:".getBytes(UTF_8);
    private static final byte[] END_CH_MARKER = "\n[id:".getBytes(UTF_8);
    private static final Pattern PORT       = Pattern.compile("^(\\d{1,5})$");
    private static final Pattern COLON_PORT = Pattern.compile("^:(\\d{1,5})$");
    private static final Pattern ADDRESS    = Pattern.compile("^.*?[^:](:\\d+)?$");
    private static final int DATA_INDENT = "99m59s999_999us >> ".length();

    private @Spec CommandSpec spec;
    private @Mixin LogOptions logOptions;
    private @Mixin OutputOptions outputOptions;

    @Option(names = {"--remote", "-r"}, showDefaultValue = Help.Visibility.NEVER,
            description = "Select channels whose with the given remote address and/or " +
                          "TCP/UDP port. If only a number or a ':' followed by a number is " +
                          "given, will select only by the given remote port. If there is no " +
                          "trailing :PORT, will select all channels with the given remote " +
                          "IPv4/IPv6 host, regardless of the remote port. If an IP v4/v6 address" +
                          "followed by :PORT is given only channels to that host at that remote " +
                          "port will be selected.")
    void setRemote(String string) {
        remote = parseEndpoint(string, " - R:/", "]", "--remote");
    }
    private byte @Nullable[] remote;

    @Option(names = {"--local", "-l"}, showDefaultValue = Help.Visibility.NEVER,
            description = "Select channels whose with the given local address and/or " +
                    "TCP/UDP port. If only a number or a ':' followed by a number is " +
                    "given, will select only by the given local port. If there is no " +
                    "trailing :PORT, will select all channels with the given local " +
                    "IPv4/IPv6 host, regardless of the local port. If an IP v4/v6 address" +
                    "followed by :PORT is given only channels to that host at that local " +
                    "port will be selected.")
    void setLocal(String string) {
        local = parseEndpoint(string, ", L:/", " - R:", "--local");
    }
    private byte @Nullable[] local;

    private byte[] parseEndpoint(String string, String before, String after, String name) {
        Matcher m = PORT.matcher(string);
        String key;
        if (m.find() || (m=COLON_PORT.matcher(string)).find()) {
            int port = Integer.parseInt(m.group(1));
            if (port <= 0 || port > 65535)
                throw paramEx(string + " is not a valid port number");
            key = ":"+port+after;
        } else if ((m= ADDRESS.matcher(string)).find()) {
            if (m.group(1) == null)
                key = before + (string.endsWith(":") ? string : string+":");
            else
                key = before + string + after;
        } else {
            throw paramEx(name+" expects a port number, :PORT or " +
                    "an IP v4/v6 address optionally followed by :PORT, got"+string);
        }
        return key.getBytes(UTF_8);
    }

    @Option(names = {"--channel", "-c"}, description = "Netty short text channel id " +
            "(a 32-bit hexadecimal integer, optionally prefixed with 0x).")
    void setChannelId(String id) {
        String str = id.toLowerCase();
        str = (str.startsWith("0x") ? "[id: " : "[id: 0x") + str;
        channelId = str.getBytes(UTF_8);
    }
    private byte[] channelId;

    public enum Side {
        IN,
        OUT,
        BOTH;

        private static final byte[]   IN_ARROW = " >> ".getBytes(UTF_8);
        private static final byte[]  OUT_ARROW = " << ".getBytes(UTF_8);
        private static final byte[] BOTH_ARROW =     "".getBytes(UTF_8);

        private byte[] arrow() {
            return switch (this) {
                case   IN ->   IN_ARROW;
                case  OUT ->  OUT_ARROW;
                case BOTH -> BOTH_ARROW;
            };
        }
    }

    @Option(names = {"--side", "-s"}, description = "Restrict direction of messages " +
            "displayed on the output. BOTH forbids --no-timing")
    void setSide(Side side) { this.sideArrow = (this.side = side).arrow(); }
    private Side side = Side.BOTH;
    private byte[] sideArrow = Side.BOTH.arrow();

    @Option(names = "--timing", negatable=true, defaultValue="true",  fallbackValue="true",
            description = "Whether relative timing information should be retained in the output")
    void setShowTiming(boolean v) {
        showTiming = v;
        skipOnData = v ? 0 : DATA_INDENT;
    }
    private boolean showTiming;
    private int skipOnData;

    @Option(names = "--single-channel", negatable = true,
            description = "Omits the channel description line from the output and requires " +
                    "that a single channel matches the selection criteria. If more than one " +
                    "channel matches the criteria, the operation will fail with no output.")
    private boolean singleChannel;

    @Option(names = "--chunk-bytes", description = "Input file will be read in chunks of this size")
    private int chunkBytes = 1024*1024;

    @Parameters(arity = "1", paramLabel = "INPUT",
            description = "Path to an existing NettyChannelDebugger dump file")
    private Path input;

    private boolean atEmptyLine;
    private @Nullable FileOutputChunk filtered;

    private ParameterException paramEx(String msg) {
        return new ParameterException(spec.commandLine(), msg);
    }

    @Override public Void call() throws Exception {
        if (side == Side.BOTH && !showTiming)
            throw paramEx("\"--side BOTH\" forbids --no-timing");
        try (var reader = new AsyncReader(input, chunkBytes);
             var writer = new AsyncWriter(outputOptions, chunkBytes)) {
            int channelCount = 0;
            boolean atBody = false;
            for (FileChunk c = null; (c = reader.nextChunk(c)) != null; ) {
                for (int i = 0, len = c.len; i < len; i++) {
                    if (atBody)
                        i = processChannel(c, i, writer);
                    if (i < len) {
                        atBody = (i = findChannel(c, i)) < len;
                        if (atBody) {
                            if (singleChannel) {
                                if (channelCount > 0)
                                    throw paramEx("--single-channel, but multiple channels match");
                                // skip channel id line
                                i = Math.min(len, c.skipUntil(i, len, (byte)'\n')+1);
                            }
                            ++channelCount;
                        }
                    }
                }
            }
            writer.end();
        } catch (AsyncIOException e) {
            if (e.getCause() instanceof AppException ae)
                throw ae;
            throw e;
        }
        return null;
    }

    private int findChannel(FileChunk chunk, int begin) {
        int len = chunk.len;
        for (int i = begin, eol; i < len; i = eol+1) {
            while ((i=chunk.skipUntil(i, len, BEGIN_CH_MARKER)) < len) {
                if (i == begin || chunk.get(i-1) == '\n')
                    break; // found a channel
                i += BEGIN_CH_MARKER.length;
            }
            if (i >= len)
                return len; // not found
            eol = chunk.skipUntil(i, len, (byte)'\n');
            if (channelId == null || chunk.has(i, channelId)) {
                if (remote == null || chunk.skipUntil(i, eol, remote) < eol) {
                    if (local == null || chunk.skipUntil(i, eol, local) < eol)
                        return singleChannel ? eol : i;
                }
            }
        }
        return len; // no matching channel in chunk
    }

    private int processChannel(FileChunk in, int begin, AsyncWriter writer)
            throws AppException, AsyncIOException {
        // chunk may start by ending the current channel body
        if (atEmptyLine && in.has(begin, BEGIN_CH_MARKER)) {
            atEmptyLine = false;
            if (filtered != null && filtered.rope.len > 0) {
                --filtered.rope.len;
                writer.write(filtered);
                filtered = null;
            }
            return begin;
        }
        atEmptyLine = false;

        int len = in.len;
        if (side == Side.BOTH) {
            int eoc = in.skipUntil(begin, len, END_CH_MARKER);
            if (eoc == len && len > 1 && in.get(len-2) == '\n' && in.get(len-1) == '\n')
                atEmptyLine = true;
            writer.writeSync(in.sendBuffer(begin, eoc));
            return eoc;
        } // else: filter IN/OUT

        var filtered = this.filtered;
        if (filtered == null)
            filtered = writer.emptyChunk();
        else
            this.filtered = null;
        boolean active = filtered.rope.len > 0;
        int i = begin;
        for (int nextLine, arrow, c0; i < len; i = nextLine) {
            nextLine = Math.min(len, in.skipUntil(i, len, (byte)'\n')+1);
            if ((c0 = in.get(i)) == ' ') {
                if (active) filtered.rope.append(in, i+skipOnData, nextLine);
            } else if (c0 == '\n' && in.has(i, END_CH_MARKER)) {
                active = false;
                if (in.has(i, END_CH_MARKER)) {
                    break;
                } else if (i == len-1) {
                    atEmptyLine = true;
                    break;
                }
            } else {
                active = (arrow = in.skipUntil(i, nextLine, sideArrow)) < nextLine;
                if (active)
                    filtered.rope.append(in, showTiming ? 0 : arrow+sideArrow.length, nextLine);
            }
        }
        if (active) this.filtered = filtered;
        else        writer.write(filtered);
        return i;
    }

    private static final class AsyncWriter extends AsyncIO<FileOutputChunk, OutputOptions> {
        private final Condition canWriteSync = newCondition();
        private @MonotonicNonNull WritableByteChannel out;

        public AsyncWriter(OutputOptions outputOptions, int chunkBytes) {
            super(outputOptions);
            putEmpty(new FileOutputChunk(chunkBytes));
            putEmpty(new FileOutputChunk(chunkBytes));
            putEmpty(new FileOutputChunk(chunkBytes));
        }

        /** Gets an empty chunk that can be filled and given to {@link #write(FileOutputChunk)} */
        public FileOutputChunk emptyChunk() throws AppException, AsyncIOException {
            return takeEmpty();
        }

        /** Schedule {@code chunk} for async writing to the destination file. */
        public void write(FileOutputChunk chunk) { putFull(chunk); }

        /** Notifies that there will be no more {@link #write(FileOutputChunk)} or
         * {@link #writeSync(ByteBuffer)} calls and the output stream should be flushed and closed. */
        public void end() {
            lock();
            try {
                ended = true;
                hasFull.signal();
            } finally { unlock(); }
            boolean interrupted = false;
            while (true) {
                try {
                    ioThread.join();
                    break;
                } catch (InterruptedException e) { interrupted = true; }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }

        /**
         * Write the bytes in {@code bb} synchronously after any previously scheduled
         * {@link #write(FileOutputChunk)} completes. The calling thread will block until
         * contents have been safely copied out of {@code bb}.
         * @param bb A {@link ByteBuffer} whose bytes from {@link ByteBuffer#position()} until
         *           {@link ByteBuffer#limit()} will be written to the destination
         */
        public void writeSync(ByteBuffer bb) throws AppException, AsyncIOException {
            lock();
            try {
                while ((hasFull() || out == null)) {
                    if (checkEnded()) throw new IllegalStateException("end() called");
                    canWriteSync.awaitUninterruptibly();
                }
                out.write(bb);
            } catch (IOException e) {
                throw new AppException("Could not write to"+path+": "+e.getMessage());
            } finally {
                unlock();
            }
        }

        @Override public void run() {
            try (var out = path.open()) {
                this.out = out;
                for (FileOutputChunk chunk; (chunk=takeFull()) != null; putEmpty(chunk)) {
                    out.write(chunk.buffer());
                    chunk.rope.len = 0;
                }
            } catch (Throwable t) { onIOError(t); }
        }
    }


    private static final class AsyncIOException extends Exception {
        public AsyncIOException(Object path, Throwable cause) {
            super(cause.getClass().getSimpleName()+" during IO on "+path+": "+cause.getMessage(),
                  cause);
        }
    }

    private static abstract class AsyncIO<T, O> extends ReentrantLock
            implements Runnable, AutoCloseable {
        private static final AtomicInteger nextId = new AtomicInteger();
        protected final Condition hasFull = newCondition(), hasEmpty = newCondition();
        protected boolean closed, ended;
        private @Nullable T empty0, empty1, empty2, full0, full1, full2;
        protected @Nullable Throwable error;
        protected final Thread ioThread;
        public final O path;

        public AsyncIO(O path) {
            this.path = path;
            String name;
            if (AsyncReader.class.desiredAssertionStatus()) {
                name = path.toString();
                if (name.length() > 28)
                    name = "AsyncIO-..."+name.substring(name.length()-25);
                else
                    name = "AsyncIO-"+name;
            } else {
                name = "AsyncIO"+nextId.getAndIncrement();
            }
            this.ioThread = new Thread(this, name);
            this.ioThread.start();
        }

        /**
         * Stops the IO thread and releases all resources.
         *
         * <p>Once this method is called, all {@link FileChunk}s are invalidated and reading
         * from them will fail.</p>
         */
        @Override public final void close() {
            lock();
            try {
                if (closed) return;
                closed = true;
                hasEmpty.signal();
                hasFull.signal();
            } finally {
                unlock();
            }
            if (closed) return;
            closed = true;
            try {
                ioThread.join();
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            afterClose();
        }

        protected void afterClose() {}

        protected void onIOError(Throwable t) {
            try {
                if (t instanceof AppException) {
                    log.debug("Exception while reading from {}", path, t);
                } else {
                    log.error("Unexpected exception while reading from {}", path, t);
                }
            } catch (Throwable ignored) {}
            lock();
            try {
                error = t;
                hasFull.signal();
            } finally { unlock(); }
        }

        protected void onIOEnd() {
            lock();
            try {
                ended = true;
                hasFull.signal();
            } finally { unlock(); }
        }

        protected boolean hasFull() { return full0 != null || full1 != null || full2 != null; }

        protected void putFull(T chunk) {
            lock();
            try {
                if      (full0 == null) full0 = chunk;
                else if (full1 == null) full1 = chunk;
                else if (full2 == null) full2 = chunk;
                else                    throw new IllegalStateException("No space");
                hasFull.signal();
            } finally { unlock(); }
        }

        /** Adds a {@link T} for consumption by {@link #run()}.
         *  Does not {@link Condition#awaitUninterruptibly()} */
        protected void putEmpty(T c) {
            lock();
            try {
                if      (empty0 == null) empty0 = c;
                else if (empty1 == null) empty1 = c;
                else if (empty2 == null) empty2 = c;
                else                     throw new IllegalStateException("No space");
                hasEmpty.signal();
            } finally { unlock(); }
        }

        protected boolean checkEnded() throws AppException, IllegalStateException, AsyncIOException {
            if (error != null) {
                switch (error) {
                    case FileNotFoundException _, NoSuchFileException _
                            -> throw new AppException(path+" not found (as a file)");
                    case IOException e
                            -> throw new AppException("IO error on "+path+": "+e.getMessage());
                    default -> throw new AsyncIOException(path, error);
                }
            } else if (ended) {
                return true;
            } else if (closed) {
                throw new IllegalStateException("closed");
            }
            return false;
        }

        protected T takeFull() throws AppException, AsyncIOException {
            T chunk;
            lock();
            try {
                while (true) {
                    if ((chunk=full0) != null) {
                        full0 = full1; full1 = full2; full2 = null;
                        return chunk;
                    } else if ((chunk=full1) != null) {
                        full1 = full2; full2 = null;
                        return chunk;
                    } else if ((chunk=full2) != null) {
                        full2 = null;
                        return chunk;
                    } else if (checkEnded()) {
                        return null;
                    }
                    hasFull.awaitUninterruptibly();
                }
            } finally { unlock(); }
        }

        /**
         * Takes a {@link T} added by {@link #putEmpty(T)}.
         * {@link Condition#awaitUninterruptibly()} on {@code hasEmpty} if required.
         *
         * @return a non-null {@link T} with containing no data.
         */
        protected T takeEmpty() throws AppException, AsyncIOException {
            T c = null;
            lock();
            try {
                while (c == null) {
                    if      ((c = empty0) != null) empty0 = null;
                    else if ((c = empty1) != null) empty1 = null;
                    else if ((c = empty2) != null) empty2 = null;
                    else if (checkEnded())         throw new IllegalStateException("IO ended");
                    else                           hasEmpty.awaitUninterruptibly();
                }
            } finally { unlock(); }
            return c;
        }
    }

    private static final class AsyncReader extends AsyncIO<FileChunk, Path> {
        private final Arena arena = Arena.ofShared();

        public AsyncReader(Path path, int chunkBytes) {
            super(path);
            putEmpty(new FileChunk(arena.allocate(chunkBytes)));
            putEmpty(new FileChunk(arena.allocate(chunkBytes)));
            putEmpty(new FileChunk(arena.allocate(chunkBytes)));
        }

        @Override protected void afterClose() { arena.close(); }

        /**
         * Gets the next {@link FileChunk}, waiting if necessary.
         *
         * @param chunk The previous {@link FileChunk} returned by this method or {@code null}
         *              if this is the first call. This must be given, else both the calling
         *              thread and the IO thread will starve.
         * @return A {@link FileChunk} with bytes that follow the one in {@link FileChunk}
         *         returned by the previous call to this method (on the same instance). The
         *         returned chunk will usually end in '\n', unless it is the last chunk and
         *         the last input byte is not a '\n'. Once there are no more inputs, {@code null}
         *         is returned.
         * @throws IllegalStateException if {@link #close()} has been called.
         */
        public @Nullable FileChunk nextChunk(@Nullable FileChunk chunk)
                throws AppException, AsyncIOException {
            if (chunk != null) {
                chunk.len = 0;
                putEmpty(chunk); // lock()/unlock() allows waiting run() to take chunk
            }
            return takeFull();
        }

        @Override public void run() {
            try (var ch = Files.newByteChannel(path, READ)) {
                long chunkPosition = 0;
                int chunkNumber = 0;
                FileChunk c = null;
                while (true) {
                    if (c == null) // if c != null, contains partial line
                        c = takeEmpty();
                    int partialLen = 0, len = c.len, read = ch.read(c.recvBuffer());
                    if (read > 0) { // if read additional bytes, trim partial line at end
                        c.len = len += read;
                        int eol = c.skipUntilLast(0, len, (byte)'\n');
                        if (eol < len) { // has newline
                            ++eol; // newline is part of the line
                            partialLen = len - eol;
                            len = eol;
                        } else if (len == c.capacity()) {
                            throw new AppException(path+" has lines >"+len+", increase --chunk-bytes");
                        }
                    }

                    // deliver chunk
                    c.len          = len;
                    c.chunkNumber  = chunkNumber++;
                    c.firstBytePos = chunkPosition;
                    chunkPosition += len;
                    putFull(c);

                    // copy partial line to next chunk
                    if (partialLen > 0) {
                        FileChunk next = takeEmpty();
                        copy(c.segment, len, next.segment, 0, partialLen);
                        (c = next).len = partialLen;
                    } else if (read == -1) {
                        onIOEnd();
                        break; // reached EOF, all bytes delivered
                    } else {
                        c = null;
                    }
                }
            } catch (Throwable t) {
                onIOError(t);
            }
        }
    }
}
