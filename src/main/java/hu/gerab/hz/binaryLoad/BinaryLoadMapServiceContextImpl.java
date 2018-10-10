package hu.gerab.hz.binaryLoad;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.*;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.query.PartitionScanRunner;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultProcessorRegistry;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is mainly delegating for a wrapped {@link MapServiceContext}, but replaces recordStores for
 * {@link BinaryLoad} annotated maps with {@link BinaryLoadRecordStore} to allow the load feature.
 */
public class BinaryLoadMapServiceContextImpl implements MapServiceContext {

    public static final String PARTITION_INFO_FILE = "partitions";

    private final ILogger logger;
    private final MapServiceContext serviceContext;

    private final Map<String, Boolean> annotationCache = new HashMap<>();
    private final Map<String, Map<Integer, BinaryLoadRecordStore>> binaryLoadRecordStoreMap = new ConcurrentHashMap<>();

    // The Service init is not running on the partition threads, so these need to be properly published
    private boolean binaryLoadEnabled = false;
    private boolean binaryLoadPartitionCountValid;
    private Path storageDir;

    public BinaryLoadMapServiceContextImpl(MapServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        logger = serviceContext.getNodeEngine().getLogger(BinaryLoadMapServiceContextImpl.class);
    }

    @Override
    public RecordStore createRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader) {

        InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();

        String name = mapContainer.getName();
        boolean isAnnotationPresent = annotationCache.computeIfAbsent(name, cacheName -> isBinaryLoadAnnotationPresent(mapContainer));
        boolean isBinaryStore = inMemoryFormat == InMemoryFormat.BINARY;

        if (binaryLoadEnabled && isAnnotationPresent && isBinaryStore) {
            ILogger logger = getNodeEngine().getLogger(BinaryLoadRecordStore.class);
            logger.fine("Creating Binary load enabled Record store for cache=" + name + ", partition=" + partitionId);
            BinaryLoadRecordStore binaryLoadRecordStore = new BinaryLoadRecordStore(mapContainer, partitionId, keyLoader, logger, binaryLoadPartitionCountValid, storageDir);
            binaryLoadRecordStoreMap.computeIfAbsent(name, n -> new ConcurrentHashMap<>()).put(partitionId, binaryLoadRecordStore);
            return binaryLoadRecordStore;
        } else {
            ILogger logger = getNodeEngine().getLogger(DefaultRecordStore.class);
            logger.fine("Skip creating Binary load Record store for cache=" + name + ", partition=" + partitionId + ", inMemoryFormat=" + inMemoryFormat + ", annotationPresent=" + isAnnotationPresent);
            return new DefaultRecordStore(mapContainer, partitionId, keyLoader, logger);
        }
    }

    // this is only expected to be called once, but we need to publish the updated variables properly to the partition threads...
    public synchronized void init(boolean binaryLoadEnabled, Path storageDir) {
        this.binaryLoadEnabled = binaryLoadEnabled;
        this.storageDir = storageDir;
        int partitionCount = serviceContext.getNodeEngine().getPartitionService().getPartitionCount();
        binaryLoadPartitionCountValid = validatePartitionInfo(storageDir, partitionCount);
        if (!binaryLoadPartitionCountValid) {
            logger.warning("Persisted partition count missing or does not match current partition count=" + partitionCount + ". Binary loading will be skipped!");
        }
    }

    @Override
    public void shutdown() {
        persistBinaryInfo();
        serviceContext.shutdown();
    }

    private void persistBinaryInfo() {
        if (!binaryLoadRecordStoreMap.isEmpty() && getNodeEngine().getPartitionService().getPartitionCount() > 0) {
            try {
                persistPartitionInfo(storageDir, getNodeEngine().getPartitionService().getPartitionCount());
                //TODO this should be invoked on the partition threads instead of the current thread/generic executor pool( they may not be available during shutdown though )
                binaryLoadRecordStoreMap.values().stream()
                        .flatMap(map -> map.values().stream())
                        .forEach(BinaryLoadRecordStore::persist);
            } catch (IOException e) {
                logger.severe("Failed to persist binary data for caches.", e);
            }
        }
    }

    private static void persistPartitionInfo(Path storageDir, int partitionCount) throws IOException {
        Path file = storageDir.resolve(PARTITION_INFO_FILE);
        Files.createDirectories(storageDir);

        try (PrintWriter out = new PrintWriter(new FileOutputStream(file.toFile()))) {
            out.write(Integer.toString(partitionCount));
            out.close();
        }
    }

    private boolean validatePartitionInfo(Path storageDir, int partitionCount) {
        Path file = storageDir.resolve(PARTITION_INFO_FILE);
        if (Files.exists(file)) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile())))) {
                int persistedPartitionCount = Integer.parseInt(in.readLine().trim());
                return persistedPartitionCount == partitionCount;
            } catch (IOException e) {
                logger.severe("Failed to validate partition count. Binary loading will be disabled.");
                return false;
            }
        }
        return false;
    }

    private static boolean isBinaryLoadAnnotationPresent(MapContainer mapContainer) {
        Object storageImpl = Optional.of(mapContainer)
                .map(MapContainer::getMapStoreContext)
                .map(MapStoreContext::getMapStoreWrapper)
                .map(MapStoreWrapper::getImpl)
                .orElse(null);

        return BinaryLoadUtils.isAnnotationPresent(BinaryLoad.class, storageImpl);
    }

    //////////////////////////////////////////// Delegate methods //////////////////////////////////

    @Override
    public Object toObject(Object data) {
        return serviceContext.toObject(data);
    }

    @Override
    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        return serviceContext.toData(object, partitionStrategy);
    }

    @Override
    public Data toData(Object object) {
        return serviceContext.toData(object);
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return serviceContext.getMapContainer(mapName);
    }

    @Override
    public Map<String, MapContainer> getMapContainers() {
        return serviceContext.getMapContainers();
    }

    @Override
    public PartitionContainer getPartitionContainer(int partitionId) {
        return serviceContext.getPartitionContainer(partitionId);
    }

    @Override
    public void initPartitionsContainers() {
        serviceContext.initPartitionsContainers();
    }

    @Override
    public void clearMapsHavingLesserBackupCountThan(int partitionId, int backupCount) {
        serviceContext.clearMapsHavingLesserBackupCountThan(partitionId, backupCount);
    }

    @Override
    public void clearPartitionData(int partitionId) {
        serviceContext.clearPartitionData(partitionId);
    }

    @Override
    public MapService getService() {
        return serviceContext.getService();
    }

    @Override
    public void clearPartitions(boolean onShutdown) {
        serviceContext.clearPartitions(onShutdown);
    }

    @Override
    public void destroyMapStores() {
        serviceContext.destroyMapStores();
    }

    @Override
    public void flushMaps() {
        serviceContext.flushMaps();
    }

    @Override
    public void destroyMap(String mapName) {
        serviceContext.destroyMap(mapName);
    }

    @Override
    public void reset() {
        serviceContext.reset();
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName) {
        return serviceContext.getRecordStore(partitionId, mapName);
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName, boolean skipLoadingOnCreate) {
        return serviceContext.getRecordStore(partitionId, mapName, skipLoadingOnCreate);
    }

    @Override
    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return serviceContext.getExistingRecordStore(partitionId, mapName);
    }

    @Override
    public Collection<Integer> getOwnedPartitions() {
        return serviceContext.getOwnedPartitions();
    }

    @Override
    public void reloadOwnedPartitions() {
        serviceContext.reloadOwnedPartitions();
    }

    @Override
    public AtomicInteger getWriteBehindQueueItemCounter() {
        return serviceContext.getWriteBehindQueueItemCounter();
    }

    @Override
    public ExpirationManager getExpirationManager() {
        return serviceContext.getExpirationManager();
    }

    @Override
    public void setService(MapService mapService) {
        serviceContext.setService(mapService);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return serviceContext.getNodeEngine();
    }

    @Override
    public MergePolicyProvider getMergePolicyProvider() {
        return serviceContext.getMergePolicyProvider();
    }

    @Override
    public MapEventPublisher getMapEventPublisher() {
        return serviceContext.getMapEventPublisher();
    }

    @Override
    public MapQueryEngine getMapQueryEngine(String name) {
        return serviceContext.getMapQueryEngine(name);
    }

    @Override
    public QueryRunner getMapQueryRunner(String name) {
        return serviceContext.getMapQueryRunner(name);
    }

    @Override
    public QueryOptimizer getQueryOptimizer() {
        return serviceContext.getQueryOptimizer();
    }

    @Override
    public LocalMapStatsProvider getLocalMapStatsProvider() {
        return serviceContext.getLocalMapStatsProvider();
    }

    @Override
    public MapOperationProvider getMapOperationProvider(String name) {
        return serviceContext.getMapOperationProvider(name);
    }

    @Override
    public MapOperationProvider getMapOperationProvider(MapConfig mapConfig) {
        return serviceContext.getMapOperationProvider(mapConfig);
    }

    @Override
    public Extractors getExtractors(String mapName) {
        return serviceContext.getExtractors(mapName);
    }

    @Override
    public void incrementOperationStats(long startTime, LocalMapStatsImpl localMapStats, String mapName, Operation operation) {
        serviceContext.incrementOperationStats(startTime, localMapStats, mapName, operation);
    }

    @Override
    public boolean removeMapContainer(MapContainer mapContainer) {
        return serviceContext.removeMapContainer(mapContainer);
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy(String mapName, PartitioningStrategyConfig config) {
        return serviceContext.getPartitioningStrategy(mapName, config);
    }

    @Override
    public void removePartitioningStrategyFromCache(String mapName) {
        serviceContext.removePartitioningStrategyFromCache(mapName);
    }

    @Override
    public PartitionContainer[] getPartitionContainers() {
        return serviceContext.getPartitionContainers();
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        serviceContext.onClusterStateChange(newState);
    }

    @Override
    public PartitionScanRunner getPartitionScanRunner() {
        return serviceContext.getPartitionScanRunner();
    }

    @Override
    public ResultProcessorRegistry getResultProcessorRegistry() {
        return serviceContext.getResultProcessorRegistry();
    }

    @Override
    public MapNearCacheManager getMapNearCacheManager() {
        return serviceContext.getMapNearCacheManager();
    }

    @Override
    public QueryCacheContext getQueryCacheContext() {
        return serviceContext.getQueryCacheContext();
    }

    @Override
    public String addListenerAdapter(String cacheName, ListenerAdapter listenerAdaptor) {
        return serviceContext.addListenerAdapter(cacheName, listenerAdaptor);
    }

    @Override
    public String addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName) {
        return serviceContext.addListenerAdapter(listenerAdaptor, eventFilter, mapName);
    }

    @Override
    public String addLocalListenerAdapter(ListenerAdapter listenerAdaptor, String mapName) {
        return serviceContext.addLocalListenerAdapter(listenerAdaptor, mapName);
    }

    @Override
    public IndexCopyBehavior getIndexCopyBehavior() {
        return serviceContext.getIndexCopyBehavior();
    }

    @Override
    public void interceptAfterGet(String mapName, Object value) {
        serviceContext.interceptAfterGet(mapName, value);
    }

    @Override
    public Object interceptPut(String mapName, Object oldValue, Object newValue) {
        return serviceContext.interceptPut(mapName, oldValue, newValue);
    }

    @Override
    public void interceptAfterPut(String mapName, Object newValue) {
        serviceContext.interceptAfterPut(mapName, newValue);
    }

    @Override
    public Object interceptRemove(String mapName, Object value) {
        return serviceContext.interceptRemove(mapName, value);
    }

    @Override
    public void interceptAfterRemove(String mapName, Object value) {
        serviceContext.interceptAfterRemove(mapName, value);
    }

    @Override
    public String generateInterceptorId(String mapName, MapInterceptor interceptor) {
        return serviceContext.generateInterceptorId(mapName, interceptor);
    }

    @Override
    public void addInterceptor(String id, String mapName, MapInterceptor interceptor) {
        serviceContext.addInterceptor(id, mapName, interceptor);
    }

    @Override
    public void removeInterceptor(String mapName, String id) {
        serviceContext.removeInterceptor(mapName, id);
    }

    @Override
    public Object interceptGet(String mapName, Object value) {
        return serviceContext.interceptGet(mapName, value);
    }

    @Override
    public boolean hasInterceptor(String mapName) {
        return serviceContext.hasInterceptor(mapName);
    }

    @Override
    public String addLocalEventListener(Object mapListener, String mapName) {
        return serviceContext.addLocalEventListener(mapListener, mapName);
    }

    @Override
    public String addLocalEventListener(Object mapListener, EventFilter eventFilter, String mapName) {
        return serviceContext.addLocalEventListener(mapListener, eventFilter, mapName);
    }

    @Override
    public String addLocalPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        return serviceContext.addLocalPartitionLostListener(listener, mapName);
    }

    @Override
    public String addEventListener(Object mapListener, EventFilter eventFilter, String mapName) {
        return serviceContext.addEventListener(mapListener, eventFilter, mapName);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        return serviceContext.addPartitionLostListener(listener, mapName);
    }

    @Override
    public boolean removeEventListener(String mapName, String registrationId) {
        return serviceContext.removeEventListener(mapName, registrationId);
    }

    @Override
    public boolean removePartitionLostListener(String mapName, String registrationId) {
        return serviceContext.removePartitionLostListener(mapName, registrationId);
    }
}