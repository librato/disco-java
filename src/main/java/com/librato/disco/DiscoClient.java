package com.librato.disco;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Keeps track of a set of services registered under a specific Zookeeper node
 */
@SuppressWarnings("unused")
public class DiscoClient<T> {
    private static final Logger log = LoggerFactory.getLogger(DiscoClient.class);
    private static final String serviceNodesFormat = "/services/%s/nodes";
    private final CuratorFramework framework;
    private final SelectorStrategy selector;
    private final String serviceNodesPath;
    private final Cache<ChildData, Node<T>> deserializedNodeCache;
    private final Decoder<T> decoder;
    private final StarterStopper starterStopper = new StarterStopper();
    private final IStateCache cache;

    public DiscoClient(CuratorFramework framework,
                       String serviceName,
                       SelectorStrategy selector,
                       Decoder<T> decoder) {
        this(framework, serviceName, selector, decoder, null);
    }

    public DiscoClient(CuratorFramework framework,
                       String serviceName,
                       SelectorStrategy selector,
                       Decoder<T> decoder,
                       ILevel2CacheStrategy cacheStrat) {
        this.framework = framework;
        this.selector = selector;
        this.decoder = decoder;
        serviceNodesPath = String.format(serviceNodesFormat, serviceName);
        deserializedNodeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build();
        PathChildrenStateCache zkStateCache = new PathChildrenStateCache(framework, serviceName, serviceNodesPath);
        this.cache = new Level2StateCache(serviceName, zkStateCache, cacheStrat);
    }

    public void start() throws Exception {
        starterStopper.start();
        cache.start();
    }

    public void stop() throws Exception {
        starterStopper.stop();
        cache.stop();
    }

    public boolean isStarted() {
        return starterStopper.isStarted();
    }

    public List<Node<T>> getAllNodes() {
        List<ChildData> data = cache.getCurrentData();
        if (data.isEmpty()) {
            return Collections.emptyList();
        }
        List<Node<T>> nodes = new ArrayList<>(data.size());
        for (ChildData child : data) {
            nodes.add(toNode(child));
        }
        return nodes;
    }

    public Optional<Node<T>> getServiceNode() {
        return nextChildData().transform(new Function<ChildData, Node<T>>() {
            public Node<T> apply(ChildData input) {
                return toNode(input);
            }
        });
    }

    Node<T> toNode(final ChildData data) {
        try {
            return deserializedNodeCache.get(data, new Callable<Node<T>>() {
                @Override
                public Node<T> call() throws Exception {
                    return _toNode(data);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    Node<T> _toNode(ChildData data) {
        String path = pathFromData(data);
        // This is somewhat hacky support for ipv6 with the same host:port notation
        int l = path.lastIndexOf(':');
        String host = path.substring(0, l);
        String port = path.substring(l + 1);
        T payload = null;
        if (data.getData() != null && data.getData().length > 0) {
            if (decoder == null) {
                log.warn("Data found but no decoder to parse it with");
            } else {
                try {
                    payload = decoder.decode(data.getData());
                } catch (Exception ex) {
                    decoder.handleException(ex);
                }
            }
        }
        return new Node<>(host, Integer.valueOf(port), payload);
    }

    Optional<ChildData> nextChildData() {
        final List<ChildData> children = cache.getCurrentData();
        if (children.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(selector.choose(cache.getCurrentData()));
    }

    String pathFromData(ChildData data) {
        String path = data.getPath();
        if (!path.startsWith(serviceNodesPath)) {
            throw new RuntimeException(String.format("Expected node format %s", serviceNodesPath));
        }
        // + 1 to also remove the trailing slash
        return path.substring(serviceNodesPath.length() + 1);
    }

    public int numServiceHosts() {
        return cache.getCurrentData().size();
    }

    public CuratorFramework getFramework() {
        return framework;
    }
}
