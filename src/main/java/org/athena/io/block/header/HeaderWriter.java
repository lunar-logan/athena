package org.athena.io.block.header;

import java.nio.ByteBuffer;

public interface HeaderWriter {
    void write(Header h, ByteBuffer buf);
}
