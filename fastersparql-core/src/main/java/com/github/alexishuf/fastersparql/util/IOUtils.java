package com.github.alexishuf.fastersparql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class IOUtils {
    private static final Logger log = LoggerFactory.getLogger(IOUtils.class);

    public static void ensureDir(File dir) throws IOException {
        if (dir.exists() && !dir.isDirectory())
            throw new IOException(dir+" exists as non-dir");
        else if (!dir.exists() && !dir.mkdirs())
            throw new IOException("Could not mkdir "+dir);
    }

    public static void writeWithTmp(File destination,
                                    ThrowingConsumer<OutputStream, IOException> writer)
            throws IOException {
        File parent = destination.getParentFile();
        if (parent == null) parent = new File("");
        if (!parent.exists() && !parent.mkdirs())
            throw new IOException("Could not mkdir "+parent);
        else if (!parent.isDirectory())
            throw new IOException(parent+" exists but is not a directory");
        File tmp = new File(parent, destination.getName() + ".tmp");
        if (tmp.exists() && !tmp.delete())
            throw new IOException("Could not remove stale"+tmp);
        try (var os = new FileOutputStream(tmp)) {
            writer.accept(os);
        }
        Files.move(tmp.toPath(), destination.toPath(), REPLACE_EXISTING);
    }

    public static String readAll(File file) throws IOException {
        try (var is = new FileInputStream(file)) {
            return new String(is.readAllBytes(), UTF_8);
        }
    }

    /**
     * Asks the kernel to flush all buffered I/O to the physical storage devices and blocks for
     * at most {@code timeoutMs} milliseconds until the flushing is completed.
     *
     * <p><strong>Warning</strong>: on some environments, this may be a no-op. The current
     * implementation tries to run spawn a process running the {@code sync} command which is
     * only available on UNIX-like systems.</p>
     *
     * @param timeoutMs maximum number of milliseconds to wait.
     */
    public static void fsync(long timeoutMs) {

        Thread sync = Thread.startVirtualThread(() -> {
            Process process = null;
            try {
                process = new ProcessBuilder("sync").redirectOutput(PIPE).redirectError(PIPE)
                        .start();
                process.waitFor(1, TimeUnit.MINUTES);
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                log.info("Will not fsync(): sync command not found");
            } finally {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                    try {
                        process.waitFor(1, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) { }
                }

            }
        });
        try {
            if (!sync.join(Duration.ofMillis(timeoutMs)))
                sync.interrupt();
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
