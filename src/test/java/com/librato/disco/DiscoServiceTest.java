package com.librato.disco;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
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
        DiscoService service = new DiscoService(framework, "myservice");
        byte[] payload = "foo bar baz bingo".getBytes();
        service.start("foo", 4321, true, payload);
        verify(os).forPath(eq("/services/myservice/nodes/foo:4321"), eq(payload));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeletesEphemeralNode() throws Exception {
        CuratorFramework framework = mockFramework();
        ExistsBuilder ceBuilder = mock(ExistsBuilder.class);
        CreateBuilder createBuilder = mock(CreateBuilder.class);
        when(framework.checkExists()).thenReturn(ceBuilder);
        when(ceBuilder.forPath("/services/myservice/nodes")).thenReturn(mock(Stat.class));
        when(ceBuilder.forPath("/services/myservice/nodes/foo:4321")).thenReturn(mock(Stat.class));
        when(framework.create()).thenReturn(createBuilder);
        when(framework.getState()).thenReturn(CuratorFrameworkState.STARTED);
        DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
        when(framework.delete()).thenReturn(deleteBuilder);
        ACLBackgroundPathAndBytesable<String> os = mock(ACLBackgroundPathAndBytesable.class);
        when(createBuilder.withMode(CreateMode.EPHEMERAL)).thenReturn(os);
        DiscoService service = new DiscoService(framework, "myservice");
        byte[] payload = "foo bar baz bingo".getBytes();
        service.start("foo", 4321, true, payload);
        verify(deleteBuilder).forPath("/services/myservice/nodes/foo:4321");
        verify(os).forPath(eq("/services/myservice/nodes/foo:4321"), eq(payload));
    }

    @Test
    public void testStop() throws Exception {
        Stat stat = new Stat();
        String path = "/services/myservice/nodes/foo:1234";
        CuratorFramework framework = mockFramework();
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(existsBuilder.forPath(path)).thenReturn(stat);
        when(framework.checkExists()).thenReturn(existsBuilder);
        DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
        when(framework.delete()).thenReturn(deleteBuilder);
        DiscoService manager = new DiscoService(framework, "myservice");
        manager.node = path;
        manager.stop();
        verify(deleteBuilder).forPath(path);
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mockFramework() {
        CuratorFramework framework = mock(CuratorFramework.class);
        when(framework.getConnectionStateListenable()).thenReturn(mock(Listenable.class));
        return framework;
    }
}
