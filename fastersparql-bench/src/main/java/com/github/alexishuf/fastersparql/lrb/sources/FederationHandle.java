package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.fed.CardinalityEstimator;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.fed.Source;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.closeAll;

public class FederationHandle implements SafeCloseable {
    private static final Logger log = LoggerFactory.getLogger(FederationHandle.class);
    public final Federation federation;
    private final AutoCloseableSet<SourceHandle> sourceHandles;

    private FederationHandle(Federation federation, AutoCloseableSet<SourceHandle> sourceHandles) {
        this.federation = federation;
        this.sourceHandles = sourceHandles;
    }

    @Override public void close() {
        closeAll(List.of(federation, sourceHandles));
    }

    public static Builder builder(File dataDir) {
        return new Builder().dataDir(dataDir);
    }

    public static final class Builder {
        private @Nullable File dataDir;
        private Set<LrbSource> subset = LrbSource.all();
        private SourceKind srcKind = SourceKind.HDT_FILE;
        private SelectorKind selKind = SelectorKind.DICT;
        private boolean waitInit = true;

        public @This Builder subset(Set<LrbSource> subset) { this.subset = subset == null ? LrbSource.all() : subset; return this; }
        public @This Builder dataDir(File dataDir)         { this.dataDir = dataDir; return this; }
        public @This Builder waitInit(boolean waitInit)    { this.waitInit = waitInit; return this; }
        public @This Builder srcKind(SourceKind kind) {
            if (kind == null) throw new IllegalArgumentException("srcKind cannot be null");
            this.srcKind = kind;
            return this;
        }
        public @This Builder selKind(SelectorKind kind) {
            if (kind == null) throw new IllegalArgumentException("selKind cannot be null");
            this.selKind = kind;
            return this;
        }

        public FederationHandle create() throws IOException {
            var federationDir = Files.createTempDirectory("fastersparql").toFile();
            File toml = new File(federationDir, "federation.toml");
            AutoCloseableSet<SourceHandle> handles = null;
            Federation federation = null;
            try {
                handles = createHandles();
                StringBuilder sb = new StringBuilder();
                for (SourceHandle h : handles)
                    sb.append(String.format("| - %-16s -> %s\n", h.source, h.specUrl));
                log.info("Starting federation with srcKind={}, selKind={} components:\n{}",
                         srcKind, selKind, sb);
                try (var w = new FileWriter(toml, StandardCharsets.UTF_8)) {
                    createFederationSpec(handles).toToml(w);
                }
                federation = Federation.load(toml);
                if (waitInit) {
                    log.info("Waiting sources init...");
                    Async.waitStage(federation.init());
                }
                return new FederationHandle(federation, handles.parallelClose(true));
            } catch (Throwable t) {
                try {
                    closeAll(Stream.of(federation, handles).filter(Objects::nonNull).iterator());
                } catch (Throwable inner) { t.addSuppressed(inner); }
                throw t;
            }
        }

        private File requireDataDir() {
            if (dataDir == null)
                throw new IllegalArgumentException("Cannot create federation without dataDir");
            if (!dataDir.exists())
                throw new IllegalArgumentException("dataDir=" + dataDir + " does not exist");
            if (!dataDir.isDirectory())
                throw new IllegalArgumentException("dataDir=" + dataDir + " is not a dir");
            return dataDir;
        }

        private AutoCloseableSet<SourceHandle> createHandles() throws IOException {
            AutoCloseableSet<SourceHandle> handles = new AutoCloseableSet<>();
            File dataDir = requireDataDir().getAbsoluteFile();
            for (LrbSource source : subset) {
                log.debug("Creating SourceHandle for {} with kind={}", source, srcKind);
                File hdt = new File(requireDataDir(), source.filename(srcKind));
                if (!hdt.exists())
                    throw new IOException("File " + hdt + " does not exist");
                if (hdt.isFile() && hdt.length() == 0)
                    throw new IOException("File " + hdt + " is empty");
                var h = srcKind.createHandle(source, dataDir);
                handles.add(h);
            }
            return handles;
        }

        private Spec createFederationSpec(AutoCloseableSet<SourceHandle> handles) {
            List<Spec> sources = new ArrayList<>();
            for (SourceHandle handle : handles) {
                sources.add(Spec.of(
                        Source.TYPE, Source.SPARQL_TYPE,
                        Source.NAME, handle.source.name().toLowerCase().replace("_", "-"),
                        "lrb-name", handle.source.name().toLowerCase(),
                        Source.URL, handle.specUrl,
                        Source.ESTIMATOR, Spec.of(CardinalityEstimator.PREFER_NATIVE, true),
                        Source.SELECTOR, selKind.createSpec(handle)));
            }
            return Spec.of(Federation.URL, "http://fed.example.org/sparql",
                           Federation.SOURCES, sources);
        }
    }
}
