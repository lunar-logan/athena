package org.athena;

import org.athena.db.Memtable;
import org.athena.db.SSTableService;
import org.athena.db.SSTables;
import org.athena.db.impl.SSTableServiceImpl;
import org.athena.io.StorageService;
import org.athena.io.impl.StorageServiceImpl;
import org.athena.service.CounterService;
import org.athena.service.impl.CounterServiceImpl;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AthenaServer {
    static final Path BASE_PATH = Paths.get(System.getProperty("user.dir"), "data");

    public static void main(String[] args) {
        StorageService storageService = new StorageServiceImpl(BASE_PATH.resolve("records.dat").toString());
        CounterService seqService = new CounterServiceImpl(BASE_PATH.resolve(".seq"));
        SSTableService ssTableService = new SSTableServiceImpl(BASE_PATH.resolve(".sst"), seqService, new SSTables(BASE_PATH.resolve(".sst")));
        Memtable memtable = new Memtable(storageService, ssTableService);

        System.out.println(memtable.get("name"));
//        memtable.write();
//        memtable.put("passion", "coding");
        System.out.println(memtable.get("passion"));
//        memtable.write();
        System.out.println(memtable.get("passion"));
    }
}
