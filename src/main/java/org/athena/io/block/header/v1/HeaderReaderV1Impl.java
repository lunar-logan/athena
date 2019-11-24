package org.athena.io.block.header.v1;

import org.athena.io.block.BlockStoreVersion;
import org.athena.io.block.header.Header;
import org.athena.io.block.header.HeaderReader;

import java.nio.ByteBuffer;

public class HeaderReaderV1Impl implements HeaderReader {
    @Override
    public Header read(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != Header.MAGIC_CONSTANT) {
            buf.rewind();
            return null;
        }

        int version = buf.getInt();
        if (version != BlockStoreVersion.BLOCK_STORE_SCHEMA_V1.getCode()) {
            throw new IllegalArgumentException();
        }
        long blockSize = buf.getLong();
        long limit = buf.getLong();
        long lastUpdate = buf.getLong();
        int position = buf.getInt();
        return new Header(magic, BlockStoreVersion.BLOCK_STORE_SCHEMA_V1.getCode(), 0, limit, null, lastUpdate, position);
    }
}
