package org.athena.db;

import org.athena.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Function;

public class SSTables {
    private static final Logger LOG = LoggerFactory.getLogger(SSTables.class);
    private final Path basePath;

    public SSTables(Path basePath) {
        Objects.requireNonNull(basePath);
        if (!basePath.toFile().exists() || !basePath.toFile().isDirectory()) {
            throw new IllegalArgumentException();
        }
        this.basePath = basePath;
    }

    private String[] getOrderedSegments() {

        String indexFileContent = FileUtils.withLock(
                basePath.resolve("index"),
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE),
                loadIndexFile()
        );


        String[] segments = indexFileContent.split("\n");
        Arrays.sort(segments);
        return segments;
    }

    private Function<FileChannel, String> loadIndexFile() {
        return channel -> {
            try {
                long fileSize = channel.size();
                ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
                channel.read(buffer);
                buffer.flip();

                return new String(buffer.array(), 0, buffer.limit());
            } catch (IOException ex) {
                LOG.error("Exception loading index file: ", ex);
            }
            return "";
        };
    }

    public MemtableValue find(String key) {
        String[] segments = getOrderedSegments();
        for (int i = segments.length - 1; i >= 0; i--) {
            String segment = segments[i];
            Path segPath = basePath.resolve(segment);
            SSTable sst = new SSTable(segPath.toString());
            MemtableValue val = sst.find(key);
            if(val != null) {
                return val;
            }
        }

        return null;
    }
}
