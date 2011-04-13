package org.apache.zookeeper.test;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class MultiTransactionTest extends ZKTestCase implements Watcher {
    private static final Logger LOG = Logger.getLogger(MultiTransactionTest.class);

    private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();

    private ZooKeeper zk;
    private ServerCnxnFactory serverFactory;

    @Before
    public void setupZk() throws IOException, InterruptedException, KeeperException {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(150);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        serverFactory = ServerCnxnFactory.createFactory(PORT, -1);
        serverFactory.startup(zks);
        LOG.info("starting up the zookeeper server .. waiting");
        Assert.assertTrue("waiting for server being up",
                ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
        zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, this);
    }

    @After
    public void shutdownServer() throws InterruptedException {
        zk.close();
        serverFactory.shutdown();
    }

    @Test
    public void testListOfOps() throws InterruptedException, KeeperException {
        try {
            zk.multi(Arrays.asList(
                    Op.create("/test/foo", new byte[]{1, 2, 3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                    Op.delete("/test/foo", 123)
            ));
            System.out.printf("after\n");
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (KeeperException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Throwable x) {
            x.printStackTrace();
        }
        System.out.printf("next\n");
    }

    @Override
    public void process(WatchedEvent event) {
        // ignore
    }
}
