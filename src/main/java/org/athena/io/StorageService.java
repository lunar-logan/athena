package org.athena.io;

import java.nio.ByteBuffer;

public interface StorageService {
    void read(long offset, ByteBuffer dest);

    String read(long offset, int len);

    /**
     * @param src
     * @return the offset at which the data was written
     */
    long write(ByteBuffer src);
}
