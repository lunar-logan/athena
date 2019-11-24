package org.athena.service.impl;

import org.athena.service.CounterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Function;

public class CounterServiceImpl implements CounterService {
    private static final Logger LOG = LoggerFactory.getLogger(CounterServiceImpl.class);
    private static final String PREFIX = "seq_";
    private final Path basePath;

    public CounterServiceImpl(Path basePath) {
        Objects.requireNonNull(basePath);
        File baseDir = basePath.toFile();
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new IllegalArgumentException();
        }
        this.basePath = basePath;
    }

    @Override
    public void createCounter(String name, long start, long step, long max) {
        if (name == null || !name.matches("[a-zA-Z]+[a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException();
        }

        Path seqPath = basePath.resolve(PREFIX + name);
        File seqFile = seqPath.toFile();
        if (seqFile.exists()) {
            LOG.info("Sequence [{}] already exists", name);
            return;
        }

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(seqPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            try (FileLock seqFileLock = fileChannel.tryLock()) {
                if (seqFileLock == null) {
                    LOG.error("Could not acquire lock on the sequence file [{}]", seqPath);
                    throw new RuntimeException("Could not acquire lock on the sequence file [" + seqPath + "]");
                }

                ByteBuffer seqInfo = getSequenceBuffer(start, step, max, start);
                fileChannel.write(seqInfo);
            }
        } catch (IOException e) {
            LOG.error("Exception while creating sequence file: ", e);
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer getSequenceBuffer(long start, long step, long max, long curVal) {
        ByteBuffer seqInfo = ByteBuffer.allocate(32);
        seqInfo.putLong(start);
        seqInfo.putLong(step);
        seqInfo.putLong(max);
        seqInfo.putLong(curVal);
        seqInfo.flip();
        return seqInfo;
    }

    private Path getSequencePath(String name) {
        if (name == null || !name.matches("[a-zA-Z]+[a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("Invalid sequence name [" + name + "]");
        }

        return basePath.resolve(PREFIX + name);
    }

    @Override
    public void deleteCounter(String name) {

    }

    @Override
    public long next(String counter) {
        Path sequenceFilePath = getSequencePath(counter);
        if (!sequenceFilePath.toFile().exists()) {
            throw new IllegalArgumentException("Counter [" + counter + "] does not exists");
        }
        return withLock(sequenceFilePath, channel -> {
            ByteBuffer seqInfo = ByteBuffer.allocate(32);
            try {
                int read = channel.read(seqInfo);
                LOG.info("{} bytes read from file [{}]", read, sequenceFilePath);

                seqInfo.flip();
                long step = seqInfo.getLong(8);
                long max = seqInfo.getLong(16);
                long cur = seqInfo.getLong(24);
                if (cur + step <= max) {
                    seqInfo.putLong(24, cur + step);
//                    LOG.info("cur={}, next={}, buf_position={}, buf_limit={}", cur, cur+step, seqInfo.position(), seqInfo.limit());
//                    LOG.info("start={}, step={}, max={}, cur={}", seqInfo.getLong(0), seqInfo.getLong(8), seqInfo.getLong(16), seqInfo.getLong(24));
//                    LOG.info("cur={}, next={}, buf_position={}, buf_limit={}", cur, cur+step, seqInfo.position(), seqInfo.limit());
                } else {
                    throw new RuntimeException("Sequence [" + counter + "] reached its max, stop iteration");
                }
                channel.write(seqInfo, 0);
                return cur;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private <R> R withLock(Path path, Function<FileChannel, R> consumer) {
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try (FileLock seqFileLock = fileChannel.lock()) {
                R result = consumer.apply(fileChannel);
                seqFileLock.release();
                return result;
            }
        } catch (IOException ex) {
            LOG.error("Exception opening file [{}]", path);
            throw new RuntimeException(ex);
        }
    }

    static final Path BASE_PATH = Paths.get(System.getProperty("user.dir"), "data");

    public static void main(String[] args) {
        CounterService counter = new CounterServiceImpl(BASE_PATH);

//        counter.createCounter("sst_id", 1, 1, Integer.MAX_VALUE);
        long sst_id = counter.next("sst_id");
        System.out.println(sst_id);

//        counter.xreateCounter("unique_id", 1, 1, Long.MAX_VALUE >> 1);
        System.out.println(counter.next("unique_id"));
    }
}
