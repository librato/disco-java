package com.librato.disco;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DiscoClientTest {
    private static final Logger log = LoggerFactory.getLogger(DiscoClientTest.class);

    CuratorFramework framework;
    DiscoClient client;

    @Test
    public void testBasicListener() throws Exception {
        final String serviceName = "alerts";
        framework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        framework.start();
        final SelectorStrategy selector = new RoundRobinSelectorStrategy();
        client = new DiscoClient(framework, serviceName, selector);
        client.start();

        framework.create().withMode(CreateMode.EPHEMERAL).forPath("/services/alerts/nodes/hello:1231");
        // Give it a bit to propagate
        Thread.sleep(100);
        assertEquals("hello:1231", client.getServiceHost().get());
    }

    @After
    public void tearDown() {
        if (client != null) {
            try {
                client.stop();
            } catch (IOException ex) {
                log.error("stopping DiscoService failed", ex);
            }
        }
        if (framework != null) {
            framework.close();
        }
    }
}
