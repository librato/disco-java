package com.librato.disco;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keeps track of a set of services registered under a specific Zookeeper node
 */
@SuppressWarnings("unused")
public class DiscoClient {
    private static final Logger log = LoggerFactory.getLogger(DiscoClient.class);
    private static final String serviceNodeFormat = "/services/%s/nodes";
    private final CuratorFramework framework;
    private final String serviceName;
    private final SelectorStrategy selector;
    private final PathChildrenCache cache;
    private final String serviceNode;
    private final LoadingCache<String, String> pathToNodeCache;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public DiscoClient(CuratorFramework framework, String serviceName, SelectorStrategy selector) {
        this.framework = framework;
        this.serviceName = serviceName;
        this.selector = selector;
        serviceNode = String.format(serviceNodeFormat, serviceName);
        cache = new PathChildrenCache(framework, serviceNode, true);
        pathToNodeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        if (!key.startsWith(serviceNode)) {
                            throw new RuntimeException(String.format("Expected node format %s", serviceNode));
                        }
                        // + 1 to also remove the trailing slash
                        return key.substring(serviceNode.length() + 1);
                    }
                });
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

    public List<Node> getAllNodes() {
        List<ChildData> data = cache.getCurrentData();
        if (data.isEmpty()) {
            return Collections.emptyList();
        }
        List<Node> nodes = new ArrayList<Node>(data.size());
        for (ChildData child : data) {
            nodes.add(toNode(child));
        }
        return nodes;
    }

    Node toNode(ChildData data) {
        String path = pathFromData(data);
        String[] split = path.split(":");
        if (split.length != 2) {
            throw new RuntimeException("Don't know how to parse node path: " + path);
        }
        return new Node(split[0], Integer.valueOf(split[1]));
    }

    public Optional<String> getServiceHost() {
        final List<ChildData> children = cache.getCurrentData();
        if (children.isEmpty()) {
            return Optional.absent();
        }
        final ChildData chosen = selector.choose(cache.getCurrentData());
        return Optional.of(pathFromData(chosen));
    }

    String pathFromData(ChildData data) {
        try {
            return pathToNodeCache.get(data.getPath());
        } catch (ExecutionException e) {
            throw Throwables.propagate(Throwables.getRootCause(e));
        }
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
