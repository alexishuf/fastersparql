package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.time.Duration;

import static java.lang.String.format;

@Slf4j
public final class HdtssContainer extends GenericContainer<HdtssContainer> {
    private static final String TEST_QUERY = "SELECT * WHERE { ?s a <http://example.org/Dummy>}";
    private static final String ENC_TEST_QUERY = UriUtils.escapeQueryParam(TEST_QUERY);

    private final @Nullable File deleteOnClose;

    public HdtssContainer(File hdtFile, Logger log) {
        this(hdtFile, log, null);
    }
    public HdtssContainer(Class<?> refClass, String resourcePath, Logger log) {
        this(log, TestUtils.extract(refClass, resourcePath));
    }

    private HdtssContainer(Logger log, File deleteOnCloseHdtFile) {
        this(deleteOnCloseHdtFile, log, deleteOnCloseHdtFile);
    }

    private HdtssContainer(File hdtFile, Logger log, @Nullable File deleteOnClose) {
        super(DockerImageName.parse("alexishuf/hdtss:jdk"));
        this.deleteOnClose = deleteOnClose;
        withFileSystemBind(hdtFile.getAbsolutePath(),
                "/data/data.hdt", BindMode.READ_ONLY)
                .withExposedPorts(8080)
                .withLogConsumer(new Slf4jLogConsumer(log)
                        .withSeparateOutputStreams()
                        .withPrefix("HDTSS container"))
                .withCommand("-port=8080", "/data/data.hdt")
                .withStartupTimeout(Duration.ofSeconds(30))
                .waitingFor(Wait.forHttp("/sparql?query=" + ENC_TEST_QUERY));
    }

    @Override public void close() {
        super.close();
        if (deleteOnClose != null && deleteOnClose.isFile() && !deleteOnClose.delete())
            log.error("Failed to delete file {}", deleteOnClose);
    }

    public SparqlEndpoint asEndpoint() {
        return asEndpoint("");
    }

    public SparqlEndpoint asEndpoint(String options) {
        int port = getMappedPort(8080);
        String sep = options == null || options.isEmpty() ? "" : "@";
        String augmented = format("%s%shttp://%s:%d/sparql", options, sep, getHost(), port);
        return SparqlEndpoint.parse(augmented);
    }
}
