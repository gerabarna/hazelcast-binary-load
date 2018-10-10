package hu.gerab.hz.binaryLoad;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class BinaryLoadService implements ManagedService {

    private static final String DEFAULT_STORAGE_DIR = "storage";

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        String prop = properties.getProperty("hu.gerab.hazelcast.binaryLoad.enabled");
        boolean binaryLoadEnabled = Boolean.parseBoolean(prop);

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        if (mapServiceContext instanceof BinaryLoadMapServiceContextImpl) {
            prop = propOrDefault(properties, "hu.gerab.hazelcast.binaryLoad.storageDir", DEFAULT_STORAGE_DIR);
            Path storageDir = Paths.get(prop);
            ((BinaryLoadMapServiceContextImpl) mapServiceContext).init(binaryLoadEnabled, storageDir);
        }
    }

    private String propOrDefault(Properties properties, String key, String defaultValue) {
        String prop;
        prop = properties.getProperty(key);
        return isBlank(prop) ? defaultValue : prop;
    }

    private boolean isBlank(String prop) {
        return prop == null || prop.trim().isEmpty();
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
