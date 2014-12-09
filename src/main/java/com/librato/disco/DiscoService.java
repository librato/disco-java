package com.librato.disco;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle starting/stopping of the zookeeper client (framework) and creation of ephemeral node for service discovery
 */
public class DiscoService {
    private static final Logger log = LoggerFactory.getLogger(DiscoService.class);
    private static final String baseNodeTemplate = "/services/%s/nodes";
    private final String baseNode;
    final CuratorFramework framework;
    String nodeName;
    int port;
    String node;
    ConnectionStateListener listener;

    public DiscoService(CuratorFramework framework, String serviceName) {
        this.framework = framework;
        this.baseNode = String.format(baseNodeTemplate, serviceName);
    }

    public void start(String nodeName, int port) throws Exception {
        Preconditions.checkArgument(framework.getState() == CuratorFrameworkState.STARTED);
        this.nodeName = nodeName;
        this.port = port;

        // Register ephemeral node as representation of this service's nodename and port
        // such as /services/myservice/nodes/192.168.1.1:8000
        this.node = baseNode + "/" + nodeName + ":" + port;
        this.listener = new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED) {
                    log.info("Re-registering with ZK as node {}", node);

                    try {
                        deleteNode();
                        createNode();
                    } catch (Exception e) {
                        log.error("Exception recreating path", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        // Ensure the parent paths exist persistently
        while (framework.checkExists().forPath(baseNode) == null) {
            log.info("Creating base node {}", baseNode);
            framework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(baseNode);
        }

        log.info("Registering with ZK as node {}", node);
        createNode();

        framework.getConnectionStateListenable().addListener(listener);
    }

    public void stop() throws Exception {
        framework.getConnectionStateListenable().removeListener(listener);
        deleteNode();
    }

    private void createNode() throws Exception {
        // TODO: Add payload capability
        framework.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(node);
    }

    private void deleteNode() throws Exception {
        if (framework.checkExists().forPath(node) != null) {
            framework.delete().forPath(node);
        }
    }
}
