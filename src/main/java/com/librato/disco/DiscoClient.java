package com.librato.disco;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keeps track of a set of services registered under a specific Zookeeper node
 */
@SuppressWarnings("unused")
public class DiscoClient<T> {
    private static final Logger log = LoggerFactory.getLogger(DiscoClient.class);
    private static final String serviceNodeFormat = "/services/%s/nodes";
    private final CuratorFramework framework;
    private final String serviceName;
    private final SelectorStrategy selector;
    private final PathChildrenCache cache;
    private final String serviceNode;
    private final Cache<ChildData, Node<T>> nodeCache;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Decoder<T> decoder;

    public DiscoClient(CuratorFramework framework, String serviceName, SelectorStrategy selector, Decoder<T> decoder) {
        this.framework = framework;
        this.serviceName = serviceName;
        this.selector = selector;
        this.decoder = decoder;
        serviceNode = String.format(serviceNodeFormat, serviceName);
        cache = new PathChildrenCache(framework, serviceNode, true);
        nodeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build();
    }

    public void start() throws Exception {
        Preconditions.checkArgument(framework.getState() == CuratorFrameworkState.STARTED);
        Preconditions.checkArgument(started.compareAndSet(false, true));
        if (framework.checkExists().forPath(serviceNode) == null) {
            framework.create().creatingParentsIfNeeded().forPath(serviceNode);
        }
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        log.info("`{}` service node added: {}", serviceName, event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        break;
                    case CHILD_REMOVED:
                        log.info("`{}` service node removed: {}", serviceName, event.getData().getPath());
                        break;
                    case CONNECTION_SUSPENDED:
                        break;
                    case CONNECTION_RECONNECTED:
                        break;
                    case CONNECTION_LOST:
                        break;
                    case INITIALIZED:
                        break;
                }
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }

    public void stop() throws IOException {
        Preconditions.checkArgument(started.compareAndSet(true, false));
        cache.close();
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
            return nodeCache.get(data, new Callable<Node<T>>() {
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
        if (!path.startsWith(serviceNode)) {
            throw new RuntimeException(String.format("Expected node format %s", serviceNode));
        }
        // + 1 to also remove the trailing slash
        return path.substring(serviceNode.length() + 1);
    }

    public int numServiceHosts() {
        return cache.getCurrentData().size();
    }

    public boolean isStarted() {
        return started.get();
    }

    public CuratorFramework getFramework() {
        return framework;
    }
}
