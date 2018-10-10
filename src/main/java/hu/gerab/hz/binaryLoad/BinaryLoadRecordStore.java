package hu.gerab.hz.binaryLoad;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

/**
 * This class extends the {@link DefaultRecordStore} with local(binary) file saving/loading capabilities
 */
public class BinaryLoadRecordStore extends DefaultRecordStore {

    private static final Path storageDir = Paths.get("").resolve("storage");
    public static final String EXTENSION = ".part";
    private final ILogger logger;

    public BinaryLoadRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.logger = logger;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new ListableStorage(recordFactory, memoryFormat, serializationService);
    }

    @Override
    public void startLoading() {
        load();
        super.startLoading();
    }

    public synchronized void persist() {
        persist(getStorageDir(), name, partitionId, ((ListableStorage<Record>) storage).getEntries(), logger);
    }

    static void  persist(Path storageDir, String name, int partitionId, Set<Map.Entry<Data, Record>> entrySet, ILogger logger) {
        Path cacheDir = storageDir.resolve(name);
        try {
            Path filePath = getFilePath(Files.createDirectories(cacheDir), partitionId);
            Files.deleteIfExists(filePath);
            Files.createFile(filePath);

            LinkedList<Map.Entry<Data, Record>> entries = new LinkedList<>(entrySet);
            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(filePath.toFile()))) {
                out.writeInt(entries.size());
                for (Map.Entry<Data, Record> entry : entries) {
                    write(out, entry);
                }
                out.close();
            }

        } catch (IOException e) {
            logger.warning("Failed to persist partition=" + partitionId + ", for cache=" + name + ", at=" + storageDir, e);
        }
    }

    private static Path getFilePath(Path cacheDir, int partitionId) {
        return cacheDir.resolve(Integer.toString(partitionId) + EXTENSION);
    }

    private static void write(DataOutputStream out, Map.Entry<Data, Record> entry) throws IOException {
        Data key = entry.getKey();
        Data value = (Data) entry.getValue().getValue();
        writeData(out, key);
        writeData(out, value);
    }

    private static void writeData(DataOutputStream out, Data key) throws IOException {
        byte[] bytes = key.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public synchronized void load() {
        load(getStorageDir(), name, partitionId, storage, recordFactory, logger);
    }

    static void load(Path storageDir, String name, int partitionId, Storage<Data, Record> storage, RecordFactory recordFactory, ILogger logger) {
        //TODO gerab we should check the partition size before the loading is started to make sure the map config has not changed...
        Path filePath = getFilePath(storageDir.resolve(name), partitionId);
        int expectedEntryCount = -1;
        int readCount = 0;
        try {
            if (Files.exists(filePath)) {
                try (DataInputStream in = new DataInputStream(new FileInputStream(filePath.toFile()))) {
                    expectedEntryCount = in.readInt();

                    for (; readCount < expectedEntryCount; readCount++) {
                        read(in, storage, recordFactory);
                    }
                }
                if (readCount > 0) {
                    logger.fine("Successfully loaded=" + readCount + " records from binary data for cache=" + name + ", partition=" + partitionId);
                } else {
                    logger.finest("Successfully loaded=" + readCount + " records from binary data for cache=" + name + ", partition=" + partitionId);
                }
            } else {
                logger.severe("Failed to load partition=" + partitionId + ", for cache=" + name + ". Storage file does not exist at=" + filePath);
            }
        } catch (IOException e) {
            logger.severe(format("Failed to load partition=%d, for cache=%s, at=%s, expected size=%d, read=%d", partitionId, name, filePath, expectedEntryCount, readCount), e);
        }

    }

    private static void read(DataInputStream in, Storage<Data, Record> storage, RecordFactory recordFactory) throws IOException {
        HeapData key = readData(in);
        Record record = recordFactory.newRecord(readData(in));
        storage.put(key, record);
    }

    private static HeapData readData(DataInputStream in) throws IOException {
        return new HeapData(readBytes(in));
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int arraySize = in.readInt();
        byte[] bytes = new byte[arraySize];
        in.readFully(bytes);
        return bytes;
    }

    public static Path getStorageDir() {
        return storageDir;
    }
}
