package org.athena.io.block.header;

import java.nio.ByteBuffer;

public interface HeaderReader {
    Header read(ByteBuffer buf);
}
