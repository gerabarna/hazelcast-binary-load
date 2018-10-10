package hu.gerab.hz.binaryLoad.helper;

import com.hazelcast.core.MapLoader;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import hu.gerab.hz.binaryLoad.BinaryLoad;

@BinaryLoad
public class TestDataBinaryLoadMapLoader implements MapLoader<String, String> {

    private static final Map<String, String> testKeyToDataMap = new TreeMap<>();

    static {
        testKeyToDataMap.put("Leo", "Loves Swords");
        testKeyToDataMap.put("Raph", "Loves Fighting");
        testKeyToDataMap.put("Donnie", "Loves Tech");

        testKeyToDataMap.put("Mike", "Getting smacked");
    }

    public Map<String, String> getTestKeyToDataMap() {
        return testKeyToDataMap;
    }

    @Override
    public String load(String key) {
        return getTestKeyToDataMap().get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        TreeMap<String, String> response = new TreeMap<>();
        for (String key : keys) {
            String value = getTestKeyToDataMap().get(key);
            if (value != null) {
                response.put(key, value);
            }
        }

        return response;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return getTestKeyToDataMap().keySet();
    }

}
