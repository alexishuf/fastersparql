package com.github.alexishuf.fastersparql.grep.cli;

import com.github.alexishuf.fastersparql.grep.FileScanner;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(showDefaultValues = true)
public class FileScannerOptions {
    @Option(names = "--scan-threads", description = "how many threads should be used to " +
            "concurrently scan log files. Default is the number of CPUs as reported by " +
            "Runtime.availableProcessors().")
    int threads = Runtime.getRuntime().availableProcessors();

    @Option(names = "--chunk-bytes", description = "Log files are read in chunks of at most " +
            "this many bytes.")
    int chunkBytes = 1024*1024; //1 MiB

    @Option(names = "--chunks-per-thread", description = "In the event the reader " +
            "thread outpaces the processing threads, at most threads*THIS_VALUE chunks shall " +
            "be enqueued at any point.")
    int chunksQueuedPerThread = 4;

    public FileScanner createScanner() {
        return new FileScanner(threads, chunkBytes, chunksQueuedPerThread);
    }

    public int maxTotalChunks() {
        return chunksQueuedPerThread*threads;
    }
}
