package com.librato.disco;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class HostAndPortTest {

    @Test
    public void testToString() throws Exception {
        Assert.assertThat(new HostAndPort("localhost", 8080).toString(), CoreMatchers.equalTo("localhost:8080"));
    }
}
