package org.athena.db;

import org.athena.io.StorageService;
import org.athena.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Memtable {
    private static final Logger LOG = LoggerFactory.getLogger(Memtable.class);

    private final ConcurrentNavigableMap<String, MemtableValue> partition = new ConcurrentSkipListMap<>();

    private final StorageService storageService;

    private final SSTableService ssTableService;

    public Memtable(StorageService storageService, SSTableService ssTableService) {
        this.storageService = storageService;
        this.ssTableService = ssTableService;
    }

    public void put(String key, String val) {
        long offset = storageService.write(ByteBufferUtil.wrap(val));
        LOG.info("key=\"" + key + "\", offset=" + offset + ", stored in block store");
        partition.put(key, new MemtableValue(val.length(), offset));
        LOG.info("key=\"" + key + "\", stored in memtable");
    }

    public void delete(String key) {
    }

    public String get(String key) {
        MemtableValue val = partition.get(key);
        if (val == null) {
            val = ssTableService.find(key);
        }

        if (val != null) {
            return storageService.read(val.getOffset(), val.getValueLength());
        }
        return null;
    }

    public void write() {
        ssTableService.write(partition);
        partition.clear();
    }
}
