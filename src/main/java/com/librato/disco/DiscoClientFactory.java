package com.librato.disco;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for building DiscoClients using the same {@link org.apache.curator.framework.CuratorFramework} instance
 */
public class DiscoClientFactory<T> {
    private final CuratorFramework framework;
    private final SelectorStrategy strategy;
    private final Decoder<T> decoder;
    private final ILevel2CacheStrategy l2CacheStrategy;

    /**
     * Constructor that defaults to using {@link RoundRobinSelectorStrategy} strategy
     * @param framework Initialized {@link CuratorFramework}
     */
    public DiscoClientFactory(CuratorFramework framework) {
        this(framework, new RoundRobinSelectorStrategy());
    }

    /**
     * @param framework Initialized {@link CuratorFramework}
     * @param strategy Selector for use in this factory
     */
    public DiscoClientFactory(CuratorFramework framework, SelectorStrategy strategy) {
        this.framework = framework;
        this.strategy = strategy;
        this.decoder = null;
        this.l2CacheStrategy = null;
    }

    /**
     * @param framework Initialized {@link CuratorFramework}
     * @param strategy Selector for use in this factory
     * @param decoder Decoder for use in thie factory
     */
    public DiscoClientFactory(CuratorFramework framework, SelectorStrategy strategy, Decoder<T> decoder) {
        this.framework = framework;
        this.strategy = strategy;
        this.decoder = decoder;
        this.l2CacheStrategy = null;
    }

    /**
     * @param framework Initialized {@link CuratorFramework}
     * @param strategy Selector for use in this factory
     * @param decoder Decoder for use in thie factory
     * @param l2CacheStrategy a strategy supplier for using the l2 cache. null if no caching is desired.
     */
    public DiscoClientFactory(CuratorFramework framework, SelectorStrategy strategy, Decoder<T> decoder, ILevel2CacheStrategy l2CacheStrategy) {
        this.framework = framework;
        this.strategy = strategy;
        this.decoder = decoder;
        this.l2CacheStrategy = l2CacheStrategy;
    }

    /**
     * Builds a {@link DiscoClient} for given service name, and calls
     * {@link DiscoClient#start()}
     * @param serviceName Passed into {@link DiscoClient} constructor
     * @return new initialized {@link DiscoClient} instance
     */
    public DiscoClient<T> buildClient(final String serviceName) {
        final DiscoClient<T> client;
        client = new DiscoClient<>(
                framework,
                serviceName,
                strategy,
                decoder,
                l2CacheStrategy);
        try {
            client.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return client;
    }
}
