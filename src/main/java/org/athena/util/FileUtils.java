package org.athena.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Function;

public final class FileUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
    }

    public static <R> R withLock(Path path, Set<OpenOption> openOptions, Function<FileChannel, R> channelConsumer) {
        FileChannel channel = null;
        FileLock lock = null;
        R result = null;
        try {
            channel = (FileChannel) Files.newByteChannel(path, openOptions);
            lock = channel.lock();
            result = channelConsumer.apply(channel);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    LOG.error("Exception releasing lock on [{}]", path, e);
                }
            }

            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    LOG.error("Exception closing channel on [{}]", path, e);
                }
            }
        }
        return result;
    }
}
