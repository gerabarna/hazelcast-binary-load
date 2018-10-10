package hu.gerab.hz.binaryLoad;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Resource;

import hu.gerab.hz.binaryLoad.helper.TestDataBinaryLoadMapLoader;
import hu.gerab.hz.binaryLoad.helper.TestUtils;

import static hu.gerab.hz.binaryLoad.BinaryLoadMapServiceContextImpl.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:hazelcast-context.xml")
@TestPropertySource("classpath:application.properties")
public class BinaryLoadIntegrationTest {

    public static final String BINARY_LOAD_MAP_NAME = "binaryLoadMap";
    public static final Path STORAGE_DIR = Paths.get("storage");

    @Resource(name = BINARY_LOAD_MAP_NAME)
    private Map<String, String> binaryLoadMap;

    @Autowired
    private TestDataBinaryLoadMapLoader testDataBinaryLoadMapLoader;

    @BeforeClass
    public static void setUp() throws Exception {
        // replace previously saved binary data
        Path cacheDir = STORAGE_DIR.resolve(BINARY_LOAD_MAP_NAME);
        Files.createDirectories(cacheDir);
        try (OutputStream outStream = new FileOutputStream(STORAGE_DIR.resolve(PARTITION_INFO_FILE).toFile())
        ) {
            outStream.write("4".getBytes());
        }

        for (int i = 0; i < 4; i++) {
            String fileName = i + BinaryLoadRecordStore.EXTENSION;
            String resourcePath = "previousBinaryLoadMapData" + File.separator + fileName;
            Path outputPath = cacheDir.resolve(fileName);
            try (InputStream inputStream = BinaryLoadIntegrationTest.class.getClassLoader().getResourceAsStream(resourcePath)
                 ; OutputStream outStream = new FileOutputStream(outputPath.toFile())
            ) {
                byte[] buffer = new byte[inputStream.available()];
                inputStream.read(buffer);
                outStream.write(buffer);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDirectory(STORAGE_DIR);
    }

    @Test
    public void contextOk() throws Exception {
        // check if spring context is initialised correctly
        assertThat(true, is(true));
    }

    @Test
    public void testBinaryLoad() throws Exception {
        Set<String> keys = binaryLoadMap.keySet();
        assertThat(keys.size(), equalTo(5));

        for (Map.Entry<String, String> entry : testDataBinaryLoadMapLoader.getTestKeyToDataMap().entrySet()) {
            assertEntry(entry.getKey(), entry.getValue());
        }
        assertEntry("Key 1", "Binary Only"); // only present in mapLoader data
        assertEntry("Key 5", "MapLoader 5"); // only present in the binary data
    }

    private void assertEntry(String key, String value) {
        assertThat(binaryLoadMap.get(key), is(value));
    }
}
