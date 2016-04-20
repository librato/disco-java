package com.librato.disco;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of this interface should be supplied to the Level2StateCache.
 * It supplies the TTL that should be used for the l2 cache entries and also
 * determines whether or not the l2 cache should be in a promoted state or not.
 *
 * Note that the strategy should probably not maintain any state, as it could
 * be used for various services when using a DiscoClientFactory.
 */
public interface ILevel2CacheStrategy {
    /**
     * Signals to the strategy what the current l1 cache size is
     * and also the l2 cache size.  This method should return true
     * if the l2 cache should be in a promoted state, or false otherwise.
     *
     * @param serviceName the name of the service
     * @param l1CacheSize the size of the l1 cache
     * @param l2CacheSize the size of the l2 cache
     * @param isPromoted  whether or not the l2 cache is already in a promoted state
     */
    boolean promote(String serviceName, int l1CacheSize, int l2CacheSize, boolean isPromoted);

    /**
     * How long the entries should live in the l2 cache while in a demoted state
     */
    long getTtl();

    /**
     * The time unit for the ttl
     */
    TimeUnit getTtlTimeUnit();
}
