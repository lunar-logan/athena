package org.athena.util;

import java.nio.ByteBuffer;

public final class ByteBufferUtil {
    private ByteBufferUtil() {
    }

    public static ByteBuffer wrap(String s) {
        if (s == null) {
            throw new NullPointerException();
        }
        return ByteBuffer.wrap(s.getBytes());
    }

    public static ByteBuffer wrap(int val) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(val).flip();
    }

    public static ByteBuffer wrap(long val) {
        return ByteBuffer.allocate(Long.BYTES).putLong(val).flip();
    }
}
