package com.example.distributedsystems;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final String ELECTION_NAMESPACE = "/election";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;
	private String currentZnode;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.electLeader();
		leaderElection.run();
		leaderElection.close();
		System.out.println("Closing application");
	}

	public void volunteerForLeadership() throws InterruptedException, KeeperException {
		String zonodePrefix = ELECTION_NAMESPACE + "/c_";
		String znodeFullpath = zooKeeper.create(zonodePrefix, new byte[0],
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Created znode: " + znodeFullpath);
		this.currentZnode = znodeFullpath.replace(ELECTION_NAMESPACE + "/", "");
	}

	public void electLeader() throws InterruptedException, KeeperException {
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		Collections.sort(children);
		String smallestChild = children.get(0);
		if (smallestChild.equals(currentZnode)) {
			System.out.println("Iam the leader");
			return;
		}
		System.out.println(" I am not the leader" + smallestChild + "is the leader");
	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}

	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
			case None:
				if (event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("Connected to ZooKeeper");
				} else {
					synchronized (zooKeeper) {
						zooKeeper.notifyAll();
						System.out.println("Disconnected from ZooKeeper");
					}
				}
		}
	}
}
