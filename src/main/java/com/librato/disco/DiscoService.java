package com.librato.disco;

import org.apache.curator.framework.CuratorFramework;
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

    public DiscoService(CuratorFramework framework, String serviceName, String nodeName, int port) {
        this.framework = framework;
        this.baseNode = String.format(baseNodeTemplate, serviceName);
        this.nodeName = nodeName;
        this.port = port;
    }

    public void start() throws Exception {
        framework.start();

        // Ensure the parent paths exist persistently
        while (framework.checkExists().forPath(baseNode) == null) {
            log.info("Creating base node");
            framework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(baseNode);
        }

        // Register ephemeral node as representation of this service's nodename and port
        // TODO: Add payload capability
        final String node = baseNode + "/" + nodeName + ":" + port;
        log.info("Registering with ZK as node {}", node);
        framework.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(node);
    }

    public void stop() {
        framework.close();
    }
}
