package hu.gerab.hz.binaryLoad;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import hu.gerab.hz.binaryLoad.helper.TestDataRecord;
import hu.gerab.hz.binaryLoad.helper.TestRecordFactory;
import hu.gerab.hz.binaryLoad.helper.TestUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BinaryLoadRecordStoreTest {

    private static final Path STORAGE_DIR = Paths.get("testStorage");
    private static final String TEST_CACHE_NAME = "test";

    private ILogger logger;

    @Before
    public void setUp() throws Exception {
        logger = mock(ILogger.class);
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDirectory(STORAGE_DIR);
    }



    @Test
    public void testBinarySaveAndLoad() throws Exception {
        // given a fake partition with 2 records
        HeapData key1 = createData("Michelangelo");
        TestDataRecord record1 = createRecord("Loves pizza");

        HeapData key2 = createData("Leonardo");
        TestDataRecord record2 = createRecord("Likes swords");

        Map<Data, Record> dataToRecordMap = new HashMap<>();
        dataToRecordMap.put(key1, record1);
        dataToRecordMap.put(key2, record2);

        // when the data is persisted and than loaded back
        BinaryLoadRecordStore.persist(STORAGE_DIR, TEST_CACHE_NAME, 0, dataToRecordMap.entrySet(), logger);
        Storage<Data, Record> storage = mock(Storage.class);
        BinaryLoadRecordStore.load(STORAGE_DIR, TEST_CACHE_NAME, 0, storage, new TestRecordFactory(), logger);

        // then all initial data is added to the storage properly
        verify(storage).put(key1, record1);
        verify(storage).put(key2, record2);
        verifyNoMoreInteractions(storage);
    }

    private TestDataRecord createRecord(String payload) {
        return new TestDataRecord(createData(payload));
    }

    private HeapData createData(String payload) {
        return new HeapData(payload.getBytes());
    }
}