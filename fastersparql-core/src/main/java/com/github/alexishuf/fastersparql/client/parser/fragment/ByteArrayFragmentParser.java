package com.github.alexishuf.fastersparql.client.parser.fragment;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.function.Function;

public class ByteArrayFragmentParser implements FragmentParser<byte[]> {
    public static final ByteArrayFragmentParser INSTANCE = new ByteArrayFragmentParser();

    static final class Encoder implements Function<CharSequence, byte[]> {
        private final CharsetEncoder encoder;
        private final CharBuffer carry;
        private ByteBuffer bb;

        public Encoder(Charset charset) {
            this.bb = ByteBuffer.allocate(512);
            this.carry = CharBuffer.allocate(2);
            this.encoder = charset.newEncoder().onUnmappableCharacter(CodingErrorAction.REPLACE)
                                               .onMalformedInput(CodingErrorAction.REPLACE);
        }

        @Override public byte[] apply(CharSequence in) {
            bb.clear();
            CharBuffer cb = CharBuffer.wrap(in);
            if (carry.position() > 0) {
                CharBuffer prefixed = CharBuffer.allocate(carry.position()+ in.length());
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

    private static final ThreadLocal<Encoder> threadEncoder = new ThreadLocal<>();
    @Override public byte[] parseString(CharSequence fragment, Charset charset) {
        Encoder encoder = threadEncoder.get();
        if (encoder == null || !encoder.encoder.charset().equals(charset)) {
            encoder = new Encoder(charset);
            threadEncoder.set(encoder);
        }
        return encoder.apply(fragment);
    }

    @Override public byte[] parseBytes(byte[] fragment, Charset charset) {
        return fragment;
    }
}
