package com.librato.disco;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class NodeTest {
    @Test
    public void testHostAndPort() throws Exception {
        Node<?> node = new Node<>("localhost", 8080, "{}");
        Assert.assertThat(node.getHostAndPort(), CoreMatchers.equalTo(new HostAndPort("localhost", 8080)));
    }
}
