package com.sirius.zk.study;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by pippo on 15/3/2.
 */
public class FindMaster {

	//private static Logger logger= Logger.getLogger(FindMaster.class);

	private static String hosts = "10.127.6.193:2181,10.127.6.194:2181,10.127.6.195:2181";

	private static Map<String, String> cluster = new ConcurrentHashMap<>();

	public static void main(String[] args) throws Exception {

		final ZooKeeper zk = new ZooKeeper(hosts, 1000 * 60, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				//System.out.println(event);
			}
		});

		Stat stat = zk.exists("/cluster", false);
		if (stat == null) {
			zk.create("/cluster", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		for (int i = 0; i < 3; i++) {
			Executors.newCachedThreadPool().execute(new Member("member" + i));
		}
	}

	private static class Member implements Runnable {

		private Member(String id) {
			this.id = id;
		}

		private ZooKeeper zk;

		private String id;

		private String path;

		private AtomicBoolean available = new AtomicBoolean(false);

		@Override
		public void run() {
			while (true) {
				try {
					start();
				} catch (Exception e) {
					System.out.println(id + path);
					e.printStackTrace();
				} finally {
					stop();
				}
			}

		}

		private void start() throws Exception {
			//模拟随机启动
			Thread.sleep(new Random().nextInt(1000) * 10);
			zk = new ZooKeeper(hosts, 1000, new ClusterWatcher(this));
			path = zk.create("/cluster/00",
					id.getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);

			System.out.println(String.format("%s:start as %s", id, path));
			//System.out.println(Arrays.toString(getMembers().toArray()));

			cluster.put(path, id);

			zk.exists("/cluster", true);
			zk.getChildren("/cluster", true);
			available.set(true);
			Thread.sleep(new Random().nextInt(1000) * 60);
		}

		private void stop() {
			System.out.println(String.format("%s:stop as %s", id, path));
			available.set(false);

			try {
				if (zk != null) {
					zk.delete(path, -1);
					zk.close();
				}
				zk = null;
			} catch (Exception e) {
				System.out.println(id + path);
				e.printStackTrace();
			}
		}

		private List<String> getMembers() {
			if (!available.get()) {
				return Collections.emptyList();
			}

			try {
				List<String> children = zk.getChildren("/cluster", true);
				Collections.sort(children);

				System.out.println(id + ":the cluster member is:" + Arrays.toString(children.toArray()));
				return children;
			} catch (Exception e) {
				System.out.println(id + path);
				e.printStackTrace();
				return Collections.emptyList();
			}
		}

		private void electMaster(String path) {
			if (!available.get() || path == null) {
				return;
			}

			//System.out.println(path);
			try {
				String master = getMaster();
				if (path.equals(master)) {
					System.out.println(String.format("%s:%s--%s was master", id, path, cluster.get(path)));
				} else {
					zk.setData("/cluster", path.getBytes(), -1);
					System.out.println(String.format("%s:elect %s--%s as master", id, path, cluster.get(path)));
				}

			} catch (Exception e) {
				System.out.println(id + path);
				e.printStackTrace();
			}
		}

		private String getMaster() throws Exception {
			if (!available.get()) {
				return null;
			}

			byte[] master = zk.getData("/cluster", true, null);
			return master != null ? new String(master) : null;
		}

	}

	private static class ClusterWatcher implements Watcher {

		private ClusterWatcher(Member self) {
			this.self = self;
		}

		private Member self;

		@Override
		public void process(WatchedEvent event) {
			//System.out.println("###" + event);

			List<String> members = self.getMembers();
			switch (event.getType()) {

				case NodeChildrenChanged:
					if (!members.isEmpty()) {
						String path = "/cluster/" + members.get(0);
						self.electMaster(path);
					}

					break;

				case NodeDataChanged:
					try {
						String master = self.getMaster();
						System.out.println(self.id + ":the master is:" + master + "---" + cluster.get(master));
					} catch (Exception e) {
						e.printStackTrace();
					}
			}

		}
	}

}
