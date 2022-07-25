package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
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

public final class FusekiContainer extends GenericContainer<FusekiContainer> {
    private static final Logger log = LoggerFactory.getLogger(FusekiContainer.class);
    private final @Nullable File deleteOnClose;

    public FusekiContainer(Class<?> refClass, String resourcePath, Logger log) {
        this(log, TestUtils.extract(refClass, resourcePath));
    }
    public FusekiContainer(File rdfFile, Logger log) {
        this(rdfFile, log, null);
    }
    private FusekiContainer(Logger log, File deleteOnCloseRdfFile) {
        this(deleteOnCloseRdfFile, log, deleteOnCloseRdfFile);
    }
    private FusekiContainer(File rdfFile, Logger log, @Nullable File deleteOnClose) {
        super(DockerImageName.parse("alexishuf/fuseki:3.17.0"));
        String name = rdfFile.getName();
        if (name.isEmpty())
            throw new IllegalArgumentException("Empty name for rdfFile="+rdfFile);
        this.deleteOnClose = deleteOnClose;
        //noinspection resource
        withFileSystemBind(rdfFile.getAbsolutePath(),
                "/data/" + name, BindMode.READ_ONLY)
                .withExposedPorts(3030)
                .withLogConsumer(new Slf4jLogConsumer(log)
                        .withSeparateOutputStreams()
                        .withPrefix("Fuseki container"))
                .withCommand("--file", "/data/" + name, "--port", "3030", "/ds")
                .waitingFor(new WaitAllStrategy()
                        .withStartupTimeout(Duration.ofSeconds(60))
                        .withStrategy(new AbstractWaitStrategy() {
                            @Override protected void waitUntilReady() {
                                try {
                                    Thread.sleep(10_000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
                        .withStrategy(Wait.forHttp("/ds"))
                );
    }

    @Override public void close() {
        if (deleteOnClose != null && deleteOnClose.isFile() && !deleteOnClose.delete())
            log.error("Failed to delete file {}", deleteOnClose);
    }

    public SparqlEndpoint asEndpoint() {
        int port = getMappedPort(3030);
        return SparqlEndpoint.parse(format("http://%s:%d/ds/sparql", getHost(), port));
    }
}
