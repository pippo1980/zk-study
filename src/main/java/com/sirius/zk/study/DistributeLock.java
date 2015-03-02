package com.sirius.zk.study;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by pippo on 15/3/2.
 */
public class DistributeLock {

	private static String hosts = "10.127.6.193:2181,10.127.6.194:2181,10.127.6.195:2181";

	public static void main(String[] args) {

		for (int i = 0; i < 10; i++) {
			final int _i = i;
			Executors.newCachedThreadPool().execute(new Runnable() {
				@Override
				public void run() {
					Lock lock = null;
					try {
						lock = new Lock("lock" + _i, "/distribute_lock_test");
						lock.get();
						System.out.println(String.format("%s:get the lock", lock.id));
					} catch (Exception e) {
						e.printStackTrace();
					} finally {

						if (lock != null) {
							lock.release();
						}
					}

				}
			});
		}

	}

	public static class Lock {

		public Lock(final String id, final String path) throws Exception {
			this.id = id;
			this.path = path;

			zk = new ZooKeeper(hosts, 1000 * 60, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					try {


						switch (event.getType()) {
							case NodeChildrenChanged:

								System.out.println(event);

								List<String> list = zk.getChildren(path, true);
								Collections.sort(list);

								System.out.println(String.format("%s:the lock acquire list is:%s",
										id,
										Arrays.toString(list.toArray())));

								//if (event.getPath().equals(_lock)) {
								semaphore.release();
								//}
								break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

			if (zk.exists(path, true) == null) {
				zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			available.set(true);
		}

		private String id;

		private String path;

		private ZooKeeper zk;

		private AtomicBoolean available = new AtomicBoolean(false);

		private String _lock;

		private Semaphore semaphore = new Semaphore(0);

		private void check() {
			try {
				List<String> list = zk.getChildren(path, true);
				Collections.sort(list);

				System.out.println(String.format("%s:the lock acquire list is:%s",
						id,
						Arrays.toString(list.toArray())));

				//如果最小的和当前lock相等,那么获得锁
				//否则等待并依赖watcher发送信号标激活
				if (_lock.equals(path + "/" + list.get(0))) {
					return;
				} else {
					semaphore.tryAcquire(1, TimeUnit.MINUTES);
					check();
				}

			} catch (Exception e) {
				destroy();
				throw new RuntimeException(e);
			}
		}

		public void get() {
			try {
				_lock = zk.create(path + "/_lock",
						new byte[0],
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);

				System.out.println(String.format("%s:the current lock stamp is:%s", id, _lock));

				check();
			} catch (Exception e) {
				destroy();
				throw new RuntimeException(e);
			}
		}

		public void release() {
			try {
				zk.delete(_lock, -1);
			} catch (Exception e) {
				destroy();
				throw new RuntimeException(e);
			}
		}

		public void destroy() {
			available.set(false);
			try {
				if (zk != null) {
					zk.close();
					zk = null;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
