package com.hazelcast.map.impl;

import com.hazelcast.map.impl.DefaultMapServiceFactory;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceConstructor;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapServiceContextImpl;
import com.hazelcast.map.impl.MapServiceFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;

import hu.gerab.hz.binaryLoad.BinaryLoadMapServiceContextImpl;

/**
 * This class is a based on the {@link MapServiceConstructor}, Wrapping the {@link MapServiceContext}
 * with a {@link BinaryLoadMapServiceContextImpl} to facilitate the necessary replacements for {@link hu.gerab.hz.binaryLoad.BinaryLoad}
 */
public class BinaryLoadMapServiceConstructor {

    private static final ConstructorFunction<NodeEngine, MapService> BINARY_LOAD_MAP_SERVICE_CONSTRUCTOR
            = nodeEngine -> {
        MapServiceContext defaultMapServiceContext = new MapServiceContextImpl(nodeEngine);
        BinaryLoadMapServiceContextImpl context = new BinaryLoadMapServiceContextImpl(defaultMapServiceContext);
        MapServiceFactory factory = new DefaultMapServiceFactory(context);
        return factory.createMapService();
    };

    private BinaryLoadMapServiceConstructor() {
    }

    /**
     * Returns a {@link ConstructorFunction Constructor} which will be used to create a  {@link MapService} object.
     *
     * @return {@link ConstructorFunction Constructor} which will be used to create a  {@link MapService} object.
     */
    public static ConstructorFunction<NodeEngine, MapService> getBinaryLoadMapServiceConstructor() {
        return BINARY_LOAD_MAP_SERVICE_CONSTRUCTOR;
    }
}
