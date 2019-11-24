package org.athena.db;

import java.util.SortedMap;

public interface SSTableService {
    /**
     * Converts a memtable to ss table
     * @param memtable
     */
    void write(SortedMap<String, MemtableValue> memtable);

    MemtableValue find(String key);
}
