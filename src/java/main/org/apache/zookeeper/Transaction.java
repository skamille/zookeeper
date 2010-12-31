package org.apache.zookeeper;

import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * Provides a builder style interface for doing multiple updates.  This is
 * really just a thin layer on top of Zookeeper.multi().
 */
public class Transaction {
    private ZooKeeper zk;
    private MultiTransactionRecord request = new MultiTransactionRecord();

    protected Transaction(ZooKeeper zk) {
        this.zk = zk;
    }

    public Transaction create(final String path, byte data[], List<ACL> acl,
                              CreateMode createMode) {
        request.add(new Op.Create(path, data, acl, createMode.toFlag()));
        return this;
    }

    public Transaction delete(final String path, int version) {
        request.add(new Op.Delete(path, version));
        return this;
    }

    public Transaction check(String path, int version) {
        request.add(new Op.Check(path, version));
        return this;
    }

    public Transaction setData(final String path, byte data[], int version) {
        request.add(new Op.SetData(path, data, version));
        return this;
    }

    public List<OpResult> commit() throws InterruptedException, KeeperException {
        return zk.multi_internal(request);
    }
}
