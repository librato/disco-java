package com.librato.disco;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for building DiscoClients using the same {@link org.apache.curator.framework.CuratorFramework} instance
 */
public class DiscoClientFactory {
    private final CuratorFramework framework;
    private final SelectorStrategy strategy;

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
    }

    /**
     * Builds a {@link DiscoClient} for given service name, and calls
     * {@link DiscoClient#start()}
     * @param serviceName Passed into {@link DiscoClient} constructor
     * @return new initialized {@link DiscoClient} instance
     */
    public DiscoClient buildClient(final String serviceName) {
        final DiscoClient client = new DiscoClient(framework, serviceName, strategy);
        try {
            client.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return client;
    }
}
