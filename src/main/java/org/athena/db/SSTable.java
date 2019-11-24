package org.athena.db;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable {
    public static final long MAGIC_CONSTANT = 0xfaffcafeL;
    private final DataInputStream dis;

    private SortedMap<String, MemtableValue> partition = new TreeMap<>();

    public SSTable(String path) {
        File sst = new File(path);
        if (!sst.isFile()) {
            throw new IllegalArgumentException("[" + path + "] is not a file");
        }

        try {
            this.dis = new DataInputStream(Files.newInputStream(sst.toPath()));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        load();
    }

    private synchronized void load() {
        try {
            long magic = dis.readLong();
            if (magic != MAGIC_CONSTANT) {
                throw new RuntimeException("Not a valid SS table");
            }

            int entries = dis.readInt();
            for (int i = 0; i < entries; i++) {
                Entry e = readEntry();
                partition.put(e.getKey(), e.getValue());
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MemtableValue find(String key) {
        return partition.get(key);
    }

    private Entry readEntry() throws IOException {
        int keyLen = dis.readInt();
        String key = new String(dis.readNBytes(keyLen));
        int valueLen = dis.readInt();
        long offset = dis.readLong();
        return new Entry(key, new MemtableValue(valueLen, offset));
    }

    private static final class Entry {
        private final String key;
        private final MemtableValue value;

        public Entry(String key, MemtableValue value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public MemtableValue getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    '}';
        }
    }

}
