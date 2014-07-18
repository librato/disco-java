package com.librato.disco;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle starting/stopping of the zookeeper client (framework) and creation of ephemeral node for service discovery
 */
public class DiscoService {
    private static final Logger log = LoggerFactory.getLogger(DiscoService.class);
    private static final String baseNodeTemplate = "/services/%s/nodes";
    private final String baseNode;
    final CuratorFramework framework;
    final String nodeName;
    final int port;
    final String node;

    public DiscoService(CuratorFramework framework, String serviceName, String nodeName, int port) {
        this.framework = framework;
        this.baseNode = String.format(baseNodeTemplate, serviceName);
        this.nodeName = nodeName;
        this.port = port;

        // Register ephemeral node as representation of this service's nodename and port
        // such as /services/myservice/nodes/192.168.1.1:8000
        this.node = baseNode + "/" + nodeName + ":" + port;
    }

    public void start() throws Exception {
        Preconditions.checkArgument(framework.getState() == CuratorFrameworkState.STARTED);
        // Ensure the parent paths exist persistently
        while (framework.checkExists().forPath(baseNode) == null) {
            log.info("Creating base node {}", baseNode);
            framework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(baseNode);
        }

        // TODO: Add payload capability
        log.info("Registering with ZK as node {}", node);
        framework.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(node);
    }

    public void stop() throws Exception {
        framework.delete().forPath(node);
    }
}
