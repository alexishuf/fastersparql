package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ByteArrayFragmentParser implements FragmentParser<byte[]> {
    public static final ByteArrayFragmentParser INSTANCE = new ByteArrayFragmentParser();

    @RequiredArgsConstructor
    static final class Encoder implements Throwing.Function<CharSequence, byte[]> {
        private final Future<Charset> charsetFuture;
        private CharsetEncoder encoder;
        private ByteBuffer bb;
        private CharBuffer carry;

        @EnsuresNonNull({"this.encoder", "this.bb", "this.carry"})
        private void init() throws ExecutionException, InterruptedException {
            if (encoder != null)
                return;
            this.bb = ByteBuffer.allocate(512);
            this.carry = CharBuffer.allocate(2);
            this.encoder = charsetFuture.get().newEncoder()
                                              .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                              .onMalformedInput(CodingErrorAction.REPLACE);
        }

        @Override public byte[] apply(CharSequence in) throws Exception {
            init();
            bb.clear();
            CharBuffer cb = CharBuffer.wrap(in);
            if (carry.position() > 0) {
                CharBuffer prefixed = CharBuffer.allocate(carry.position()+in.length());
                carry.flip();
                cb = prefixed.put(carry).put(cb);
                cb.flip();
            }
            while (cb.hasRemaining()) {
                CoderResult cr = encoder.encode(cb, bb, false);
                int carrySize = cb.remaining();
                if (cr.isOverflow()) {
                    bb.flip();
                    bb = ByteBuffer.allocate(bb.capacity() * 2).put(bb);
                } else if (cr.isUnderflow()) {
                    if (carrySize > 0) {
                        carry.clear();
                        carry.put(cb).limit(carrySize);
                    }
                    break;
                } else {
                    throw new IllegalStateException("Unexpected "+cr+" from encoder.encode()");
                }
            }
            return Arrays.copyOf(bb.array(), bb.position());
        }
    }

    @Override public Class<byte[]> fragmentClass() {
        return byte[].class;
    }

    @SuppressWarnings("unchecked") @Override
    public Publisher<byte[]> parseStrings(Graph<? extends CharSequence> source) {
        return new MappingPublisher<>((Publisher<CharSequence>) source.publisher(),
                                      new Encoder(source.charset()));
    }

    @Override public Publisher<byte[]> parseBytes(Graph<byte[]> source) {
        return source.publisher();
    }
}
