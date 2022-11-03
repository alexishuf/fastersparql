package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.time.Duration;

import static java.lang.String.format;

public final class HdtssContainer extends GenericContainer<HdtssContainer> {
    private static final Logger log = LoggerFactory.getLogger(HdtssContainer.class);
    private static final String TEST_QUERY = "SELECT * WHERE { ?s a <http://example.org/Dummy>}";
    private static final String ENC_TEST_QUERY = UriUtils.escapeQueryParam(TEST_QUERY);

    private final @Nullable File deleteOnClose;

    public HdtssContainer(Class<?> refClass, String resourcePath, Logger log) {
        super(DockerImageName.parse("alexishuf/hdtss:jdk"));
        File hdtFile = TestUtils.extract(refClass, resourcePath);
        this.deleteOnClose = hdtFile;
        //noinspection resource
        withFileSystemBind(hdtFile.getAbsolutePath(),
                "/data/data.hdt", BindMode.READ_ONLY)
                .withExposedPorts(8080)
                .withLogConsumer(new Slf4jLogConsumer(log)
                        .withSeparateOutputStreams()
                        .withPrefix("HDTSS container"))
                .withCommand("-port=8080", "/data/data.hdt")
                .waitingFor(new WaitAllStrategy()
                        .withStartupTimeout(Duration.ofSeconds(60))
                        .withStrategy(new AbstractWaitStrategy() {
                            @Override protected void waitUntilReady() {
                                try {
                                    Thread.sleep(10_000);
                                } catch (InterruptedException e) { throw new RuntimeException(e); }
                            }
                        })
                        .withStrategy(Wait.forHttp("/sparql?query="+ENC_TEST_QUERY)));
    }

    @Override public void close() {
        super.close();
        if (deleteOnClose != null && deleteOnClose.isFile() && !deleteOnClose.delete())
            log.error("Failed to delete file {}", deleteOnClose);
    }

    public SparqlEndpoint asEndpoint() {
        return asEndpoint(SparqlConfiguration.EMPTY);
    }

    public SparqlEndpoint asEndpoint(SparqlConfiguration cfg) {
        var method = cfg.methods().get(0);
        var scheme = method == SparqlMethod.WS ? "ws" : "http";
        int port = getMappedPort(8080);
        var suffix = method == SparqlMethod.WS ? "/ws" : "";
        var uri = format("%s://%s:%d/sparql%s", scheme, getHost(), port, suffix);
        return new SparqlEndpoint(uri, cfg);
    }
}
