package com.librato.disco;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class DiscoServiceTest {
    @SuppressWarnings("unchecked")
    @Test
    public void testStart() throws Exception {
        CuratorFramework framework = mockFramework();
        ExistsBuilder ceBuilder = mock(ExistsBuilder.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class);
        when(framework.checkExists()).thenReturn(ceBuilder);
        when(ceBuilder.forPath("/services/myservice/nodes")).thenReturn(mock(Stat.class));
        when(framework.create()).thenReturn(createBuilder);
        when(framework.getState()).thenReturn(CuratorFrameworkState.STARTED);
        ACLBackgroundPathAndBytesable<String> os = mock(ACLBackgroundPathAndBytesable.class);
        when(createBuilder.withMode(CreateMode.EPHEMERAL)).thenReturn(os);
        DiscoService service = new DiscoService(framework, "myservice", "foo", 4321);
        service.start();
        verify(os).forPath("/services/myservice/nodes/foo:4321");
    }

    @Test
    public void testStop() throws Exception {
        String path = "/services/myservice/nodes/foo:1234";
        CuratorFramework framework = mockFramework();
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(existsBuilder.forPath(path)).thenReturn(mock(Stat.class));
        when(framework.checkExists()).thenReturn(existsBuilder);
        DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
        when(framework.delete()).thenReturn(deleteBuilder);
        DiscoService manager = new DiscoService(framework, "myservice", "foo", 1234);
        manager.stop();
        verify(deleteBuilder).forPath(path);
    }

    @Test
    public void testDynamicDiscoServiceWrapperStart() throws Exception {
        CuratorFramework framework = mockFramework();
        ExistsBuilder ceBuilder = mock(ExistsBuilder.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class);
        when(framework.checkExists()).thenReturn(ceBuilder);
        when(ceBuilder.forPath("/services/myservice/nodes")).thenReturn(mock(Stat.class));
        when(framework.create()).thenReturn(createBuilder);
        when(framework.getState()).thenReturn(CuratorFrameworkState.STARTED);
        ACLBackgroundPathAndBytesable<String> os = mock(ACLBackgroundPathAndBytesable.class);
        when(createBuilder.withMode(CreateMode.EPHEMERAL)).thenReturn(os);
        DiscoService.DynamicDiscoServiceWrapper dynamicDiscoServiceWrapper = new DiscoService.DynamicDiscoServiceWrapper()
                .withFramework(framework).withServiceName("myservice").withNodeName("foo").withPort(7777);
        dynamicDiscoServiceWrapper.init();
        dynamicDiscoServiceWrapper.start();
        verify(os).forPath("/services/myservice/nodes/foo:7777");
    }

    @Test
    public void testDynamicDiscoServiceWrapperStop() throws Exception {
        String path = "/services/myservice/nodes/foo:6666";
        CuratorFramework framework = mockFramework();
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(existsBuilder.forPath(path)).thenReturn(mock(Stat.class));
        when(framework.checkExists()).thenReturn(existsBuilder);
        DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
        when(framework.delete()).thenReturn(deleteBuilder);
        DiscoService.DynamicDiscoServiceWrapper dynamicDiscoServiceWrapper = new DiscoService.DynamicDiscoServiceWrapper()
                .withFramework(framework).withServiceName("myservice").withNodeName("foo").withPort(6666);
        dynamicDiscoServiceWrapper.init();
        dynamicDiscoServiceWrapper.stop();
        verify(deleteBuilder).forPath(path);
    }

    private CuratorFramework mockFramework() {
        CuratorFramework framework = mock(CuratorFramework.class);

        when(framework.getConnectionStateListenable()).thenReturn(mock(Listenable.class));

        return framework;
    }
}
