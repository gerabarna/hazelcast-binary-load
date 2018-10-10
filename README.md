# Hazelcast binary load
This is a simple modification of Hazelcast to allow persisting to/loading from local binary files. 
This can yield significant improvements for loading sizable amounts of compressed data at the cost of local storage and shutdown time.

##Limitations
The feature is mostly useful for single node 'clusters' as the binary data for a partition needs to be on the file system. 
The modification makes no attempt to control the partition assignment between nodes, 
limiting it's usefulness to proper clusters 
( unless partition assignment to nodes are enforced by other means)

Please note that the implementation __assumes no data changes between cluster restarts__ 
( as loading the binary files essentially replaces the loading from external data sources, though entries belonging to previously unknown keys will be loaded as normal through the mapLoader), 
and no changes in the classes for the cache on which the feature is enabled. 
Thus it is only useful in scenarios where data is rarely updated.

**Note:** this feature is by no means an optimal or complete solution.

## Usage
To enable the feature add the service in your hazelcast configuration add a service section ( or set the configuration programmatically):
```
<services>
    <aervice>
        <name>BinaryLoadService</hz:name>
        <class-name>hu.gerab.hz.binaryLoad.BinaryLoadService</hz:class-name>
        <properties>
            <property name="hu.gerab.hazelcast.binaryLoad.enabled">
                true
            </property>
            <property name="hu.gerab.hazelcast.binaryLoad.storageDir">
                storage directory path
            </property>
        </properties>
    </service>
</services>
```
And place the @BinaryLoad annotation on the MapLoaders ( or MapStores ) belonging to the caches.

you may also need to define the following resource file: META-INF/services/com.hazelcast.instance.NodeExtension
with the same content as in the project.