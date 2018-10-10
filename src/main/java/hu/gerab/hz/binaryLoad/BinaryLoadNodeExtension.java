package hu.gerab.hz.binaryLoad;

import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.BinaryLoadMapServiceConstructor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;

public class BinaryLoadNodeExtension extends DefaultNodeExtension {

    public BinaryLoadNodeExtension(Node node) {
        super(node);
    }

    @Override
    public <T> T createService(Class<T> clazz) {
        if (MapService.class.isAssignableFrom(clazz)) {
            return createMapService();
        } else {
            return super.createService(clazz);
        }
    }

    private <T> T createMapService() {
        ConstructorFunction<NodeEngine, MapService> constructor = BinaryLoadMapServiceConstructor.getBinaryLoadMapServiceConstructor();
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        return (T) constructor.createNew(nodeEngine);
    }
}
