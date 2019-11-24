package org.athena.io.block.header.v1;

import org.athena.io.block.BlockStoreVersion;
import org.athena.io.block.header.Header;
import org.athena.io.block.header.HeaderWriter;

import java.nio.ByteBuffer;

public class HeaderWriterV1Impl implements HeaderWriter {
    @Override
    public void write(Header h, ByteBuffer buf) {
        buf.putInt(0, h.getMagic());
        buf.putInt(4, BlockStoreVersion.BLOCK_STORE_SCHEMA_V1.getCode());
        buf.putLong(8, 0);
        buf.putLong(16, h.getStoreLimit());
        buf.putLong(24, h.getUpdatedAt());
        buf.putInt(32, h.getPosition());
    }
}
