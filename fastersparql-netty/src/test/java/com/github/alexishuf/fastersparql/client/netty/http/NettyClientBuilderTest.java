package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.NettyClientBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient.makeRequest;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NettyClientBuilderTest {
    private static EventLoopGroup serverAcceptGroup;
    private static EventLoopGroup serverWorkerGroup;
    private static Channel serverChannel;
    private static int port;
    private static final List<Throwable> serverHandlerExceptions = Collections.synchronizedList(new ArrayList<>());
    private static final List<Throwable> clientHandlerExceptions = Collections.synchronizedList(new ArrayList<>());

    @BeforeAll
    static void beforeAll() {
        serverAcceptGroup = new NioEventLoopGroup(1);
        serverWorkerGroup = new NioEventLoopGroup();
        serverChannel = new ServerBootstrap().group(serverAcceptGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(65536))
                                .addLast(new ServerHandler());
                    }
                }).bind(0).syncUninterruptibly().channel();
        port = ((InetSocketAddress)serverChannel.localAddress()).getPort();
    }

    @AfterAll
    static void afterAll() {
        serverChannel.close().syncUninterruptibly();
        serverAcceptGroup.shutdownGracefully();
        serverWorkerGroup.shutdownGracefully();
    }

    @BeforeEach
    void setUp() {
        serverHandlerExceptions.clear();
        clientHandlerExceptions.clear();
    }

    private static class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            //read request
            HttpHeaders headers = req.headers();
            assertEquals("text/x.payload+req", headers.get(CONTENT_TYPE));
            assertEquals(Integer.toString(req.content().readableBytes()),
                         headers.get(HttpHeaderNames.CONTENT_LENGTH));
            Matcher matcher = Pattern.compile("\\?x=(\\d+)").matcher(req.uri());
            assertTrue(matcher.find(), "Missing x query param in "+req.uri());
            int x = Integer.parseInt(matcher.group(1));
            int requestedSize = Integer.parseInt(req.content().toString(UTF_8));
            assertTrue(requestedSize > 0, "requestedSize="+requestedSize);

            //build response
            ctx.writeAndFlush(createResponse(x));
            sendChunk(ctx, 0, requestedSize);
        }

        private HttpResponse createResponse(int x) {
            DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();
            responseHeaders.set(CONTENT_TYPE, "text/x.payload+res");
            responseHeaders.set("x-vnd-number", x);
            responseHeaders.set(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
            return new DefaultHttpResponse(HTTP_1_1, OK, responseHeaders);
        }

        private void sendChunk(ChannelHandlerContext ctx, int from, int totalSize) {
            int chunkEnd = Math.min(totalSize, from + 16);
            assertTrue(chunkEnd <= totalSize, "extra sendChunk() call");
            ByteBuf bb = ctx.alloc().buffer(chunkEnd - from);
            writeResponseChunk(bb, from, chunkEnd);
            if (chunkEnd == totalSize) {
                ctx.writeAndFlush(new DefaultLastHttpContent(bb));
            } else {
                ctx.writeAndFlush(new DefaultHttpContent(bb));
                ctx.executor().schedule(() -> sendChunk(ctx, from + 16, totalSize),
                        1, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            serverHandlerExceptions.add(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    private static class ClientHandler extends NettyHttpHandler {
        private CompletableFuture<String> future;
        private boolean hadResponse = false;
        private int expectNumber = -1, expectSize = -1;
        private final StringBuilder responseBuilder = new StringBuilder();

        public void setup(CompletableFuture<String> future, int expectNumber, int expectSize) {
            assertNull(this.future);
            this.future = future;
            assertEquals(-1, this.expectNumber, "already setup");
            assertEquals(-1, this.expectSize, "already setup");
            this.expectNumber = expectNumber;
            this.expectSize = expectSize;
            super.expectResponse();
        }

        @Override protected void successResponse(HttpResponse response) {
            assertFalse(hadResponse, "not the fist handleResponse()!");
            assertEquals(0, responseBuilder.length());
            hadResponse = true;
            HttpHeaders headers = response.headers();
            assertEquals("text/x.payload+res", headers.get(CONTENT_TYPE));
            assertEquals(Integer.toString(expectNumber), headers.get("x-vnd-number"));
            assertEquals("chunked", headers.get(HttpHeaderNames.TRANSFER_ENCODING));
        }

        @Override protected void content(HttpContent content) {
            assertTrue(hadResponse, "HttpContent before HttpResponse");
            String string = content.content().toString(UTF_8);
            if (string.isEmpty()) {
                assertTrue(content instanceof LastHttpContent, "if empty, chunk must be last");
            } else {
                assertTrue(string.length() <= 16,
                        "chunk is too long (" + string.length() + ")");
                assertTrue(string.matches("^[0-9a-f]+$"), "Invalid chars in " + string);
                if (!responseBuilder.isEmpty()) {
                    char lastChar = responseBuilder.charAt(responseBuilder.length() - 1);
                    int lastValue = Integer.parseInt(String.valueOf(lastChar), 16);
                    int first = Integer.parseInt(String.valueOf(string.charAt(0)), 16);
                    assertEquals((lastValue + 1) % 16, first,
                            "chunk is not contiguous with previous");
                } else {
                    assertEquals('0', string.charAt(0),
                            "First chunk must start with 0");
                }
                for (int i = 1; i < string.length(); i++) {
                    int prev = Integer.parseInt(String.valueOf(string.charAt(i - 1)), 16);
                    int curr = Integer.parseInt(String.valueOf(string.charAt(i)), 16);
                    assertEquals((prev + 1) % 16, curr,
                            "char " + i + " not contiguous in " + string);
                }
                responseBuilder.append(string);
            }

            if (content instanceof LastHttpContent) {
                assertEquals(expectSize, responseBuilder.length());
                assertTrue(future.complete(responseBuilder.toString()),
                        "future already complete");
                this.future = null;
                expectNumber = expectSize = -1;
                hadResponse = false;
                responseBuilder.setLength(0);
            }
        }

        @Override protected void error(Throwable cause) {
            future.completeExceptionally(cause);
            clientHandlerExceptions.add(cause);
            this.future = null;
            expectNumber = expectSize = -1;
            hadResponse = false;
            responseBuilder.setLength(0);
        }
    }

    private static void writeResponseChunk(ByteBuf bb, int from, int end) {
        for (int i = from; i < end; i++)
            bb.writeByte(Integer.toHexString(i%16).charAt(0));
    }

    private static String generateResponse(int size) {
        StringBuilder b = new StringBuilder(size);
        for (int i = 0; i < size; i++)
            b.append(Integer.toHexString(i % 16));
        return b.toString();
    }

    static Stream<Arguments> test() {
        List<NettyClientBuilder> builders = asList(
                //poolFIFO is NOP when !pooled
                new NettyClientBuilder().pooled(false).poolFIFO(true),
                //ocsp and startTls are NOP since we are using plain HTTP
                new NettyClientBuilder().pooled(false).ocsp(true).startTls(true),
                new NettyClientBuilder().pooled(false).shareEventLoopGroup(false),
                new NettyClientBuilder().pooled(false),
                new NettyClientBuilder().pooled(true).poolFIFO(true),
                new NettyClientBuilder().pooled(true).shareEventLoopGroup(false),
                new NettyClientBuilder().pooled(true)
        );
        int doubleThreads = Math.min(4, 2*Runtime.getRuntime().availableProcessors());
        return builders.stream()
                .flatMap(builder -> Stream.of(1, 4, 16, 17, 32, 40, 128)
                        .flatMap(size -> Stream.of(1, doubleThreads)
                                .map(clients -> arguments(size, clients, builder))));
    }

    @ParameterizedTest @MethodSource("test")
    void test(int payloadSize, int clients, NettyClientBuilder builder) throws Exception {
        String uri = "http://127.0.0.1:" + port + "/sparql";
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<String>> futures = new ArrayList<>();
        String expectedResponse = generateResponse(payloadSize);
        boolean onTime;
        try {
            preheatExecutor(clients, executor, futures);
            for (int i = 0; i < clients; i++) {
                int id = 1000+i;
                futures.add(executor.submit(() -> {
                    try (var c = builder.buildHTTP(uri, ClientHandler::new)) {
                        var req = makeRequest(HttpMethod.POST, "/endpoint?x=" + id,
                                             "text/x.payload+res",
                                             "text/x.payload+req",
                                String.valueOf(payloadSize), UTF_8);
                        var response = new CompletableFuture<String>();
                        c.request(req, (BiConsumer<Channel, ClientHandler>)
                                  (ch, h) -> h.setup(response, id, payloadSize),
                                  response::completeExceptionally);
                        return response.get();
                    }
                }));
            }
        } finally {
            executor.shutdown();
            onTime = executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        for (Future<?> f : futures)
            assertEquals(expectedResponse, f.get());
        assertTrue(serverHandlerExceptions.isEmpty());
        assertTrue(clientHandlerExceptions.isEmpty());
        assertTrue(onTime); //only assert if had no other exception

    }

    private void preheatExecutor(int clients, ExecutorService executor, List<Future<String>> futures) throws InterruptedException, ExecutionException {
        for (int i = 0; i < clients; i++) {
            int id = i;
            futures.add(executor.submit(() -> String.valueOf(id*2)));
        }
        Set<String> actualNumbers = new HashSet<>(), expectedNumbers = new HashSet<>();
        for (int i = 0, size = futures.size(); i < size; i++) {
            actualNumbers.add(futures.get(i).get());
            expectedNumbers.add(String.valueOf(i*2));
        }
        assertEquals(expectedNumbers, actualNumbers);
        futures.clear();
    }
}