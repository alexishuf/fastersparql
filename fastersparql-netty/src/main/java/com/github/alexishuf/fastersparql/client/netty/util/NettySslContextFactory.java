package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;

public class NettySslContextFactory implements Throwing.Supplier<SslContext> {
    public static final NettySslContextFactory INSTANCE = new NettySslContextFactory();

    @Override public SslContext get() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forClient();
        File collectionFile = FasterSparqlNettyProperties.trustCertCollectionFile();
        if (collectionFile != null)
            builder.trustManager(collectionFile);
        return builder.startTls(FasterSparqlNettyProperties.startTls())
                      .enableOcsp(FasterSparqlNettyProperties.ocsp()).build();
    }
}
