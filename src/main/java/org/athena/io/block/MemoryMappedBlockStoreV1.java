package org.athena.io.block;

import org.athena.io.block.header.Header;
import org.athena.io.block.header.HeaderReader;
import org.athena.io.block.header.HeaderWriter;
import org.athena.io.block.header.v1.HeaderReaderV1Impl;
import org.athena.io.block.header.v1.HeaderWriterV1Impl;
import org.athena.util.CommonUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class MemoryMappedBlockStoreV1 implements BlockStore {
    private static final long DEFAULT_BLOCK_SIZE = 1024 * 1024 * 16; // 16 KB block

    private final Path filePath;

    private final long limit;

    private final FileChannel channel;

    private final MappedByteBuffer mappedByteBuffer;

    private final Header header;

    private final HeaderReader headerReader;

    private final HeaderWriter headerWriter;


    public MemoryMappedBlockStoreV1(String filePath, long limit, HeaderReader headerReader, HeaderWriter headerWriter) throws IOException {
        Objects.requireNonNull(filePath);

        if (limit <= 0 || !CommonUtil.isPowerOf2(limit)) {
            throw new IllegalArgumentException("Limit must be a non zero power of 2");
        }

//        if (!CommonUtil.isPowerOf2(blockSize)) {
//            throw new IllegalArgumentException("Block size must be a power of 2");
//        }
//
//        if (limit < blockSize || limit % blockSize != 0) {
//            throw new IllegalArgumentException("Limit must be multiple of block size");
//        }

        this.filePath = Paths.get(filePath);

        this.limit = limit;

//        this.blockSize = blockSize;

        this.channel = (FileChannel) Files.newByteChannel(this.filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE);

        this.mappedByteBuffer = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, this.limit);

        this.headerReader = headerReader;

        this.headerWriter = headerWriter;

        Header header = headerReader.read(mappedByteBuffer);
        if (header == null) {
            header = new Header(Header.MAGIC_CONSTANT, BlockStoreVersion.BLOCK_STORE_SCHEMA_V1.getCode(), 0, limit, null, System.currentTimeMillis(), 36);
            headerWriter.write(header, mappedByteBuffer);
        }
        this.header = header;
//        System.out.println(this.header);
        this.mappedByteBuffer.position(this.header.getPosition());
//        System.out.println(mappedByteBuffer.position());
    }

    @Override
    public long write(ByteBuffer buf) {
        int offset;
        synchronized (mappedByteBuffer) {
            offset = mappedByteBuffer.position();
            mappedByteBuffer.put(buf);
            header.setPosition(mappedByteBuffer.position());
            header.setUpdatedAt(System.currentTimeMillis());
            headerWriter.write(header, mappedByteBuffer);
        }
        return offset;
    }

    @Override
    public void read(long offset, ByteBuffer dest) {
        synchronized (mappedByteBuffer) {
            for (long i = offset; i < offset + dest.limit(); i++) {
                dest.put(mappedByteBuffer.get((int) i));
            }
        }
    }


    @Override
    public long limit() {
        return limit;
    }

    @Override
    public long blockSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getStoragePath() {
        return filePath;
    }

    @Override
    public void flush() {
        mappedByteBuffer.force();
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }

    static final Path BASE_PATH = Paths.get(System.getProperty("user.dir"), "data");

    public static void main(String[] args) throws Exception {
        BlockStore store = new MemoryMappedBlockStoreV1(BASE_PATH.resolve("record1.dat").toString(), 1024 * 1024 * 16, new HeaderReaderV1Impl(), new HeaderWriterV1Impl());


        long offset = store.write(ByteBuffer.wrap("Hello Anurag".getBytes()));
        System.out.println(offset);
//
//        long offset = store.write(wrap("this is me"));
//        System.out.println(offset);
        ByteBuffer allocate = ByteBuffer.allocate(16);
        store.read(36, allocate);
        allocate.flip();
        System.out.println(new String(allocate.array(), 0, allocate.limit()));
        store.close();
    }
}
