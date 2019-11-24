package org.athena.db.impl;

import org.athena.db.MemtableValue;
import org.athena.db.SSTable;
import org.athena.db.SSTableService;
import org.athena.db.SSTables;
import org.athena.service.CounterService;
import org.athena.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

public class SSTableServiceImpl implements SSTableService {
    private static final Logger LOG = LoggerFactory.getLogger(SSTableServiceImpl.class);
    private static final String SEGMENT_FILE_NAME_PREFIX = "seg_";

    private final Path basePath;

    private final CounterService counterService;

    private final SSTables ssTables;

    public SSTableServiceImpl(Path basePath, CounterService counterService, SSTables ssTables) {
        Objects.requireNonNull(basePath);
        if (!basePath.toFile().exists() || !basePath.toFile().isDirectory()) {
            throw new IllegalArgumentException();
        }
        this.basePath = basePath;
        this.counterService = counterService;
        this.ssTables = ssTables;
        this.counterService.createCounter("sst_id_seq", 1, 1, Integer.MAX_VALUE);
    }

    @Override
    public void write(SortedMap<String, MemtableValue> memtable) {
        long seqVal = counterService.next("sst_id_seq");
        String segmentName = SEGMENT_FILE_NAME_PREFIX + seqVal;

        Path segmentPath = basePath.resolve(segmentName);
        LOG.info("Storing to [{}]", segmentPath);

        try (FileChannel channel = (FileChannel) Files.newByteChannel(segmentPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            writeHeader(channel, memtable);
            writeEntries(channel, memtable);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Update the index file
        FileUtils.withLock(
                basePath.resolve("index"),
                Set.of(StandardOpenOption.APPEND, StandardOpenOption.CREATE),
                channel -> {
                    try {
                        channel.write(ByteBuffer.wrap(segmentName.getBytes()));
                        channel.write(ByteBuffer.wrap("\n".getBytes()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                });


    }

    private void writeEntries(FileChannel channel, SortedMap<String, MemtableValue> memtable) throws IOException {
        for (Map.Entry<String, MemtableValue> e : memtable.entrySet()) {
            writeEntry(channel, e);
        }
    }

    private void writeEntry(FileChannel channel, Map.Entry<String, MemtableValue> e) throws IOException {
        ByteBuffer keyBuf = ByteBuffer.allocate(4);
        keyBuf.putInt(e.getKey().length());
        keyBuf.flip();
        channel.write(keyBuf);

        ByteBuffer key = ByteBuffer.wrap(e.getKey().getBytes());
        channel.write(key);

        ByteBuffer valLen = ByteBuffer.allocate(4);
        valLen.putInt(e.getValue().getValueLength());
        valLen.flip();
        channel.write(valLen);

        ByteBuffer valOffset = ByteBuffer.allocate(8);
        valOffset.putLong(e.getValue().getOffset());
        valOffset.flip();
        channel.write(valOffset);
    }

    private void writeHeader(FileChannel channel, SortedMap<String, MemtableValue> memtable) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(12);
        header.putLong(SSTable.MAGIC_CONSTANT);
        header.putInt(memtable.size());
        header.flip();
        channel.write(header);
    }

    @Override
    public MemtableValue find(String key) {
        return ssTables.find(key);
    }
}
