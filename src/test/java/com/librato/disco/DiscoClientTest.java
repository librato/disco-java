package com.librato.disco;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DiscoClientTest {
    private static final Logger log = LoggerFactory.getLogger(DiscoClientTest.class);
    private static final SelectorStrategy strategy = new RoundRobinSelectorStrategy();
    private static final Decoder<MyObject> decoder = new Decoder<MyObject>() {
        @Override
        public MyObject decode(byte[] bytes) {
            return new MyObject(bytes);
        }

        @Override
        public void handleException(Exception ex) {
            log.error("Exception when decoding", ex);
        }
    };

    CuratorFramework framework;
    DiscoClient<MyObject> client;

    @Test
    public void testBasicListener() throws Exception {
        final String serviceName = "myservice";
        framework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        framework.start();
        final DiscoClientFactory<MyObject> factory = new DiscoClientFactory<>(framework, strategy, decoder);
        client = factory.buildClient(serviceName);

        assertEquals(0, client.numServiceHosts());

        byte[] payload = "This is the payload!".getBytes();
        framework.create().withMode(CreateMode.EPHEMERAL).forPath("/services/myservice/nodes/hello:1231", payload);
        // Give it a bit to propagate
        Thread.sleep(100);
        assertEquals(new Node<>("hello", 1231, new MyObject(payload)), client.getServiceNode().get());
        assertEquals(1, client.numServiceHosts());
    }

    @Test
    public void testGetServiceNode() throws Exception {
        framework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        framework.start();
        final DiscoClientFactory<MyObject> factory = new DiscoClientFactory<>(framework);
        client = factory.buildClient("myservice");
        assertEquals(0, client.numServiceHosts());
        byte[] payload = "lol".getBytes();
        framework.create().withMode(CreateMode.EPHEMERAL).forPath("/services/myservice/nodes/hello1:1231", payload);
        // Give it a bit to propagate
        Thread.sleep(100);
        Optional<Node> node = client.getServiceNode();
        assertTrue(node.isPresent());
        assertEquals("hello1", node.get().host);
        assertEquals(1231, node.get().port);
        // No decoder means payload is null
        assertNull(node.get().payload);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDecoderFailure() throws Exception {
        framework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        framework.start();
        Decoder<MyObject> failingDecoder = mock(Decoder.class);
        RuntimeException exception = new RuntimeException();
        when(failingDecoder.decode(any(byte[].class))).thenThrow(exception);
        final DiscoClientFactory<MyObject> factory = new DiscoClientFactory<>(framework, strategy, failingDecoder);
        client = factory.buildClient("myservice");
        assertEquals(0, client.numServiceHosts());
        byte[] payload = "lol".getBytes();
        framework.create().withMode(CreateMode.EPHEMERAL).forPath("/services/myservice/nodes/hello1:1231", payload);
        // Give it a bit to propagate
        Thread.sleep(100);
        Optional<Node> node = client.getServiceNode();
        assertTrue(node.isPresent());
        assertEquals("hello1", node.get().host);
        assertEquals(1231, node.get().port);
        // null on exception
        assertNull(node.get().payload);
        verify(failingDecoder).handleException(exception);
    }

    @Test
    public void testAllNodes() throws Exception {
        framework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        framework.start();
        final DiscoClientFactory<MyObject> factory = new DiscoClientFactory<>(framework, strategy, decoder);
        client = factory.buildClient("myservice");
        assertEquals(0, client.numServiceHosts());
        DiscoService svcA = new DiscoService(framework, "myservice");
        svcA.start("hello1", 1231, false, null); // null payload
        DiscoService svcB = new DiscoService(framework, "myservice");
        byte[] pload = "testo".getBytes();
        svcB.start("hello2", 1232, false, pload); // real payload
        // Give it a bit to propagate
        Thread.sleep(100);
        Set<Node> nodes = new HashSet<>(client.getAllNodes());
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(new Node<>("hello1", 1231, null)));
        assertTrue(nodes.contains(new Node<>("hello2", 1232, new MyObject(pload))));
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

    private static class MyObject {
        final String foo;

        private MyObject(byte[] bytes) {
            this.foo = new String(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyObject myObject = (MyObject) o;
            return Objects.equals(foo, myObject.foo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("foo", foo)
                    .toString();
        }
    }
}
