package org.athena.io.block;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface BlockStore extends AutoCloseable {
    long write(ByteBuffer buf);

    void read(long offset, ByteBuffer dest);

    long limit();

    long blockSize();

    Path getStoragePath();

    void flush();
}
