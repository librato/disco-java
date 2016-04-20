package com.librato.disco;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DiscoClientTest {
    private static final Logger log = LoggerFactory.getLogger(DiscoClientTest.class);
    private static final SelectorStrategy strategy = new RoundRobinSelectorStrategy();
    private Decoder<MyObject> decoder;

    CuratorFramework framework;
    DiscoClient<MyObject> client;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        decoder = mock(Decoder.class);
        when(decoder.decode(any(byte[].class))).thenAnswer(new Answer<MyObject>() {
            @Override
            public MyObject answer(InvocationOnMock invocation) throws Throwable {
                return new MyObject((byte[]) invocation.getArguments()[0]);
            }
        });
    }

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
        for (int i = 0; i < 100; i++) {
            client.getServiceNode();
        }
        // We want to call the decoder only once for the same data
        verify(decoder, times(1)).decode(eq(payload));
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
        Optional<Node<MyObject>> node = client.getServiceNode();
        assertTrue(node.isPresent());
        assertEquals("hello1", node.get().host);
        assertEquals(1231, node.get().port);
        // No decoder means payload is null
        assertNull(node.get().payload);
    }

    @Test
    public void testGetServiceNodeIPV6() throws Exception {
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
        framework.create().withMode(CreateMode.EPHEMERAL).forPath("/services/myservice/nodes/a64e:31ff:fe02:3f88:1231", payload);
        // Give it a bit to propagate
        Thread.sleep(100);
        Optional<Node<MyObject>> node = client.getServiceNode();
        assertTrue(node.isPresent());
        assertEquals("a64e:31ff:fe02:3f88", node.get().host);
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
        Optional<Node<MyObject>> node = client.getServiceNode();
        assertTrue(node.isPresent());
        assertEquals("hello1", node.get().host);
        assertEquals(1231, node.get().port);
        // null on exception
        assertNull(node.get().payload);
        for (int i = 0; i < 100; i++) {
            client.getServiceNode();
        }
        verify(failingDecoder, times(1)).handleException(exception);
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
        HashSet<Node<MyObject>> nodes = new HashSet<>(client.getAllNodes());
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(new Node<MyObject>("hello1", 1231, null)));
        assertTrue(nodes.contains(new Node<>("hello2", 1232, new MyObject(pload))));
    }

    @After
    public void tearDown() {
        if (client != null) {
            try {
                client.stop();
            } catch (Exception ex) {
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
