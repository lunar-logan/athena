package org.athena.io.impl;

import org.athena.io.StorageService;
import org.athena.io.block.BlockStore;
import org.athena.io.block.MemoryMappedBlockStoreV1;
import org.athena.io.block.header.v1.HeaderReaderV1Impl;
import org.athena.io.block.header.v1.HeaderWriterV1Impl;
import org.athena.util.CommonUtil;
import org.athena.util.Constants;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageServiceImpl implements StorageService {
    private final Map<String, FileChannel> pool = new ConcurrentHashMap<>();

    private static final long MIN_STORE_LIMIT = 16 * Constants.MB;

    private final BlockStore store;

    public StorageServiceImpl(String path, long storeLimit) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Invalid path");
        }

        if (storeLimit < MIN_STORE_LIMIT || !CommonUtil.isPowerOf2(storeLimit)) {
            throw new IllegalArgumentException("Store limit must be greater than 16MB and a power of 2");
        }
        try {
            this.store = new MemoryMappedBlockStoreV1(path, storeLimit, new HeaderReaderV1Impl(), new HeaderWriterV1Impl());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to initialize storage service", ex);
        }
    }

    public StorageServiceImpl(String path) {
        this(path, MIN_STORE_LIMIT);
    }

    @Override
    public void read(long offset, ByteBuffer dest) {
        store.read(offset, dest);
    }

    @Override
    public String read(long offset, int len) {
        ByteBuffer buf = ByteBuffer.allocate(len);
        read(offset, buf);
        return new String(buf.array(), 0, len);
    }

    @Override
    public long write(ByteBuffer src) {
        return store.write(src);
    }
}