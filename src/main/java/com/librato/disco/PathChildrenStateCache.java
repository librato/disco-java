package com.librato.disco;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class PathChildrenStateCache implements IStateCache {
    private static final Logger log = LoggerFactory.getLogger(PathChildrenStateCache.class);
    private final PathChildrenCache cache;
    private final CuratorFramework framework;
    private final String serviceName;
    private final String serviceNode;
    private final StarterStopper starterStopper = new StarterStopper();

    public PathChildrenStateCache(CuratorFramework framework, String serviceName, String serviceNode) {
        this.framework = framework;
        this.serviceName = serviceName;
        this.serviceNode = serviceNode;
        cache = new PathChildrenCache(framework, serviceNode, true);
    }

    @Override
    public List<ChildData> getCurrentData() {
        return cache.getCurrentData();
    }

    @Override
    public void start() throws Exception {
        starterStopper.start();
        Preconditions.checkArgument(framework.getState() == CuratorFrameworkState.STARTED);
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

    @Override
    public void stop() throws Exception {
        starterStopper.stop();
        cache.close();
    }
}
