package com.ngdata.hbaseindexer.mr.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by robert on 8/23/16.
 */
public class ZkConfigAccessor {

	public byte[] read(String zkQuorum, String zkPath) throws InterruptedException, KeeperException, IOException {
		CountDownLatch connSignal = new CountDownLatch(0);
		ZooKeeper zk = new ZooKeeper(zkQuorum, 1000, event -> {
			if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				connSignal.countDown();
			}
		});
		connSignal.await();
		Stat stat = new Stat();
		byte[] data = zk.getData(zkPath, false, stat);
		zk.close();
		return data;
	}

	public void write(byte[] bytes, String nodePath, String zkQuorum) throws Exception {
		try (CuratorFramework client = CuratorFrameworkFactory.newClient(zkQuorum, new RetryForever(1000))) {
			client.start();
			write(bytes, nodePath, client);
		}
	}

	public void write(byte[] bytes, String nodePath, CuratorFramework client) throws Exception {
		if (client.checkExists().forPath(nodePath) != null) {
			client.setData().forPath(nodePath, bytes);
		} else {
			client.create().creatingParentsIfNeeded()
					.withMode(CreateMode.PERSISTENT)
					.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
					.forPath(nodePath, bytes);
		}
	}
}
