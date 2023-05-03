package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.store.index.HdtConverter;
import com.github.alexishuf.fastersparql.store.index.Triples;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

@Command(name = "hdt2store", description = "Convert an HDT file into a directory with " +
        "the fastersparql-store indexes")
public class Hdt2Store implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(Hdt2Store.class);

    @Option(names = {"--same-dir"}, description = "Treat all parameters as HDT files " +
            "and convert generate index dirs with same basename on the same dir as the HDT file")
    private boolean sameDir = false;
    @Option(names = {"--force", "-f"}, description = "Rebuild indexes even if already existing " +
            "with matching number of triples.")
    private boolean force = false;
    @Option(names = {"-n", "--dry-run"}, description = "Only log what actions would be taken " +
            "and produce no side-effects")
    private boolean dryRun = false;

    @Option(names = {"--temp-dir"}, description = "Use the given directory to store temporary files")
    private Path tempDir = null;

    @Parameters(paramLabel = "HDT_DESTDIR_PAIRS", description = "A pair of HDT files and " +
            "the index dir to be created for that HDT file. If --same-dir was given, all " +
            "parameters will be treated as HDT files and the DESTDIR will be computed from " +
            "the HDT file name")
    private List<File> params = List.of();

    public static void main(String[] args) {
        System.exit(run(args));
    }

    public static int run(String[] args) {
        return new CommandLine(new Hdt2Store()).execute(args);
    }

    @Override public Void call() throws Exception {
        long callStart = System.nanoTime();
        if (tempDir != null) {
            File file = tempDir.toFile();
            if (file.exists() && file.isDirectory())
                return fail("--temp-dir {} exists as non-dir", file);
            if (!file.exists() && !file.mkdirs())
                return fail("Could not mkdir --temp-dir {}", file);
        }
        handleSameDirOpt();
        if (params.size() == 0 || (params.size() & 1) == 1)
            return fail("Expected pairs of HDT and dest dir as parameters, got {}", params.size());
        for (int i = 0, n = params.size(); i < n; i += 2) {
            File hdt = params.get(i), dest = params.get(i+1);
            if (!hdt.isFile())
                return fail("{} is not a file", hdt);
            if (dest.exists() && !dest.isDirectory())
                return fail("{} already exists as non-directory", dest);
            if (!dest.exists() && !dest.mkdirs())
                return fail("Could not mkdir {}", dest);
            long start = System.nanoTime();
            log.info("Starting conversion of {} into {}...", hdt, dest);
            convert(hdt, dest);
            long ms = (System.nanoTime() - start) / 1_000_000L;
            log.info("Converted/checked {} -> {} in {}m{}.{}s",
                     hdt, dest, ms/60_000, ms/1_000, ms%1_000);
        }
        long callMs = (System.nanoTime()-callStart)/1_000_000L;
        log.info("Completed in {}m{}.{}s", callMs/60_000, callMs/1_000, callMs%1_000);
        return null;
    }

    private Void fail(String fmt, Object... args) {
        log.error(fmt, args);
        return null;
    }

    private void handleSameDirOpt() {
        if (sameDir)  {
            ArrayList<File> list = new ArrayList<>(params.size());
            for (File hdt : params) {
                list.add(hdt);
                var basename = hdt.getName().replaceAll("\\.hdt$", "");
                var parent = hdt.getParentFile();
                list.add(parent == null ? new File(basename) : new File(parent, basename));
                params = list;
            }
        }
    }

    private void convert(File hdtFile, File dest) throws IOException {
        log.debug("Mapping {}", hdtFile);
        try (HDT hdt = HDTManager.mapHDT(hdtFile.getAbsolutePath())) {
            Path destPath = dest.toPath();
            Path tempDirPath = tempDir == null ? destPath : tempDir;
            long triples = hdt.getTriples().getNumberOfElements();
            if (!force) {
                boolean valid = Stream.of("spo", "pso", "ops").parallel()
                        .allMatch(n -> validate(triples, destPath.resolve(n)));
                if (valid) {
                    log.info("{} looks valid, will not rebuild", dest);
                    return;
                }
            }
            if (dryRun) {
                log.info("--dry-run: Would convert {} into {}", hdtFile, dest);
            } else {
                log.info("Converting {} triples in {} into {}", triples, hdt, dest);
                HdtConverter.convert(hdt, destPath, tempDirPath);
            }
        }
    }

    private boolean validate(long expected, Path path) {
        if (!Files.exists(path)) return false;
        try (Triples triples = new Triples(path)) {
            if (triples.triplesCount() != expected) {
                log.debug("{} deemed invalid: expected {} triples, got {}",
                          path, expected, triples.triplesCount());
                return false;
            }
            return true;
        } catch (Throwable t) {
            return false;
        }

    }

}