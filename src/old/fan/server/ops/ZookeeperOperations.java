package old.fan.server.ops;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import old.fan.cfg.Cfg;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;

public class ZookeeperOperations {

	// ReadWriteLock curTxnFileLock = new ReentrantReadWriteLock();
	// ReadWriteLock replyFileCreatedLock = new ReentrantReadWriteLock();
	// ReadWriteLock replyFileDeletedLock = new ReentrantReadWriteLock();
	private ZooKeeper zk;
	private String zkHost;
	private final String PATH_SEPARATOR = "/";
	private final String ROOT = "/txn";
	private final String TXN_FILE_PREFIX = "txn_";
	private final String REPLY_FILE_PREFIX = "reply_";

	private final String Separator_Between_Diff_Host = "#";
	private final String Separator_Between_NNID_IP = "@";
	private final String HOST_MAP_DIR = "/mapdir";

	private final String STATE_TXN_SEPARATOR = "#";

	private final long RETRY_CONNECT_INTERVAL = 1000;

	private boolean isCoordinator = false;
	private String coordinator, nnId, nnAddress;
	private boolean allReplyFileCreated = false, allReplyFileDeleted = false;

	private AtomicLong processingTxnId = new AtomicLong(0);
	private AtomicLong txnIdNumber = new AtomicLong(1);
	/*
	 * private String currentTxnFile = ROOT + PATH_SEPARATOR + TXN_FILE_PREFIX +
	 * processingTxnId;
	 */
	private String processingPath = ROOT + PATH_SEPARATOR + TXN_FILE_PREFIX
			+ processingTxnId;
	private String creatingPath = ROOT + PATH_SEPARATOR + TXN_FILE_PREFIX
			+ txnIdNumber.get();

	Map<String, String> hostMap = new TreeMap<String, String>();

	CountDownLatch cnctLatch = new CountDownLatch(1);
	ConnectedWatcher wh = new ConnectedWatcher(cnctLatch) {
		@Override
		public void process(WatchedEvent event) {
			if (event.getState() == KeeperState.Expired
					|| event.getState() == KeeperState.Disconnected) {
				boolean flag = false;
				while (!flag) {
					connect(zkHost);
					flag = true;
				}
			}
			if (event.getState() == KeeperState.SyncConnected) {
				cnctLatch.countDown();
			}
		}
	};

	/*
	 * Watcher coordinatorCreateTxnIdFileWatche = new CoordinatorWatcher(
	 * hostMap.size(), zk, currentTxnFile, "opsType"); Watcher watcherOnRoot =
	 * new CoordinatorWatcher(hostMap.size(), zk, ROOT, "opsType");
	 * 
	 * Watcher nnListenOnTxnIdFileWatcher = new NamenodeWatcher(nnId, zk,
	 * currentTxnFile);
	 */
	public void connect(String host) {
		zkHost = host;
		boolean done = false;
		while (!done) {
			try {
				zk = new ZooKeeper(host, ClientBase.CONNECTION_TIMEOUT, wh);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (zk != null) {
				done = true;
			}
		}
	}

	public Map<String, String> initNN(String zkHost, String nnAddress) {
		this.zkHost = zkHost;
		this.nnAddress = nnAddress;
		// System.out.println("0");
		this.nnId = getNNID();
		// System.out.println("1");
		connect(zkHost);
		registerToZK(nnAddress);
		return hostMap;

	}

	public void registerToZK(String nnHostIp) {
		boolean done = false;
		String path;
		String hostStr, tmp;
		String[] hosts, record;
		String nnId_ip = nnId + Separator_Between_NNID_IP + nnHostIp;

		while (!done) {
			try {
				if (zk.exists(HOST_MAP_DIR, true) == null) {
					path = zk.create(HOST_MAP_DIR, nnId_ip.getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					if (path.equals(HOST_MAP_DIR)) {
						hostMap.put(nnId, nnHostIp);
						done = true;
						coordinator = getAndSetCoordinator();
					}
				} else {
					hostStr = new String(zk.getData(HOST_MAP_DIR, true, null));
					tmp = hostStr;
					if (!hostStr.contains(nnId)) {// if the NN with id number
													// "nnId" is not in the
													// /hostmap file, append it
													// in.
						hostStr += Separator_Between_Diff_Host + nnId_ip;
						zk.setData(HOST_MAP_DIR, hostStr.getBytes(), -1);
						hostMap.put(nnId, nnHostIp);
					}
					hosts = tmp.split(Separator_Between_Diff_Host);
					for (String host : hosts) {
						record = host.split(Separator_Between_NNID_IP);
						nnId = record[0].trim();
						nnHostIp = record[1].trim();
						hostMap.put(nnId, nnHostIp);
					}
					coordinator = getAndSetCoordinator();
					done = true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public String getAndSetCoordinator() {
		coordinator = Cfg.coordinator;
		return coordinator;
	}

	/**
	 * How to generate the globally unique id number to identify a NN.
	 * 
	 * @return
	 */
	public String getNNID() {
		String NNID = Cfg.nnId;
		return NNID;
	}

	public Map<String, String> getHostMap() {
		return hostMap;
	}

	/**
	 * Transactions file's number, as the increments of the transaction, the
	 * number increment
	 * 
	 * @return
	 */
	private long nextTxnId() {
		txnIdNumber.incrementAndGet();
		return txnIdNumber.get();
	}

	/**
	 * Get and Set the path name of currentTxnFile.
	 * 
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private synchronized String getAndSetCreatingPath() {
		// System.out.println("ZookeeperOperations:Enter:getAndSet");
		List<String> list = null;
		boolean done = false;
		String tmp;
		while (!done) {
			try {
				list = zk.getChildren(ROOT, false);
				done = true;
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (list.size() > 0) {
			// curTxnFileLock.writeLock().lock();
			tmp = Collections.max(list);
			tmp = tmp.substring(TXN_FILE_PREFIX.length(), tmp.length());
			txnIdNumber.set(Long.parseLong(tmp));
			txnIdNumber.incrementAndGet();
			creatingPath = ROOT + PATH_SEPARATOR + TXN_FILE_PREFIX
					+ txnIdNumber.get();
			allReplyFileDeleted = false;
			// curTxnFileLock.writeLock().unlock();
			// System.out.println("ZookeeperOperations:Out:getAndSet");
			System.out.println("creatingPath:" + creatingPath);
			return creatingPath;
		} else {
			// //curTxnFileLock.writeLock().lock();
			txnIdNumber.incrementAndGet();
			creatingPath = ROOT + PATH_SEPARATOR + TXN_FILE_PREFIX
					+ txnIdNumber.get();
			allReplyFileDeleted = false;
			System.out.println("creatingPath:" + creatingPath);
			// curTxnFileLock.writeLock().unlock();
			// System.out.println("ZookeeperOperations:Out:getAndSet");
			return creatingPath;
		}
	}

	private void setCurrentTxnFile(String processingPath) {
		// curTxnFileLock.writeLock().lock();
		this.processingPath = processingPath;
		// curTxnFileLock.writeLock().unlock();
	}

	/************************** OperationsForCoordinator *************************************/
	class OperationsForCoordinator {
	}

	// public String addTxn

	public boolean createTxnFile(String op) {
		// boolean ret = false;
		System.out.println("ZookeeperOperations::createTxnFile");
		boolean done = false;
		TxnState state = TxnState.PREPARE_LOCK;
		String data = state.text + STATE_TXN_SEPARATOR + op;
		String path = null;
		// curTxnFileLock.readLock().lock();
		String currentTxnFile = getAndSetCreatingPath();
		// curTxnFileLock.readLock().unlock();
		while (!done) {
			try {
				System.out.println("currentTxnFile:" + currentTxnFile);
				if (zk.exists(currentTxnFile, false) == null) {
					// System.out.println("Create " + currentTxnFile);
					path = zk.create(currentTxnFile, data.getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).trim();
					done = path.equals(currentTxnFile);
					System.out.println(currentTxnFile + " has created!");
				} else {
					done = true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return done;
	}

	public boolean hasReceivedAllReplyFile() {
		if (allReplyFileCreated) {
			return true;
		}
		boolean done = false;
		int replyCount = 0;
		String path = null, state = null;

		// curTxnFileLock.readLock().lock();
		path = processingPath;
		// curTxnFileLock.readLock().unlock();
		while (!done) {
			try {
				replyCount = zk.getChildren(path, true).size();
				state = new String(zk.getData(path, false, null))
						.split(STATE_TXN_SEPARATOR)[0].trim();
				if (state.equals(TxnState.RELEASE_LOCK.text)
						|| (state.equals(TxnState.PREPARE_LOCK.text) && (replyCount == (hostMap
								.size() - 1)))) {
					allReplyFileCreated = true;
					done = true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return allReplyFileCreated;
	}

	/**
	 * Update the state of TxnIdFile, do not process whether all reply file
	 * created by NN
	 * 
	 * @param state
	 * @return true if update successfully, else false
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean updateTxnFileState(String state) {
		boolean ret = false, done = false, flag = false;
		String data = state.trim();
		String path = null;
		// curTxnFileLock.readLock().lock();
		path = processingPath;// currentTxnFile;
		// curTxnFileLock.readLock().unlock();
		while (!done) {
			try {
				if (flag) {
					Thread.sleep(1000);
				}
				if (zk.exists(path, false) != null) {
					data += STATE_TXN_SEPARATOR
							+ new String(zk.getData(path, false, null))
									.split(STATE_TXN_SEPARATOR)[1].trim();
					ret = (zk.setData(path, data.getBytes(), -1) == null) ? false
							: true;
				}
				done = ret;
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}

	public boolean allReplyFileDeleted() {
		if (allReplyFileDeleted) {
			return allReplyFileDeleted;
		} else {
			boolean done = false, flag = false;
			String state = null;
			int replyCount = 0;
			// curTxnFileLock.readLock().lock();
			String path = processingPath;// currentTxnFile;
			// curTxnFileLock.readLock().unlock();
			while (!done) {
				try {
					if (flag) {
						Thread.sleep(1000);
					}
					if (zk.exists(path, false) != null) {
						replyCount = zk.getChildren(path, false).size();
						state = new String(zk.getData(path, false, null))
								.split(STATE_TXN_SEPARATOR)[0].trim();
						if (state.equals(TxnState.RELEASE_LOCK.text)
								&& (replyCount == 0)) {
							allReplyFileDeleted = true;
						}
					}
					flag = true;
					done = allReplyFileDeleted;
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return allReplyFileDeleted;
		}
	}

	/**
	 * We don't process the logical of whether all reply file deleted by NN
	 * before deleteTxnFile,
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public boolean deleteTxnFile() {
		boolean ret = false, done = false;
		String path = null;
		// curTxnFileLock.readLock().lock();
		path = processingPath;// currentTxnFile;
		// curTxnFileLock.readLock().unlock();
		while (!done) {
			try {
				if (zk.exists(path, false) != null) {
					zk.delete(path, -1);
					ret = true;
				}
				done = true;
				// System.out.println("sleep 3000ms");
				allReplyFileCreated = false;
				allReplyFileDeleted = false;
				Thread.sleep(3000);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}

	/********************* OperationsForNN *************************************/
	class OperationsForNN {
	}

	/**
	 * 没有判断txn_id文件是否已经是PREPARE_LOCK状态
	 * 
	 * @param nnId
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean createReplyFile(long nnId) {
		String state = "LOCKED";
		String tmp = null;
		boolean done = false;
		// curTxnFileLock.readLock().lock();
		// System.out.println("currentTxnFile:" + currentTxnFile);
		String path = processingPath + PATH_SEPARATOR + REPLY_FILE_PREFIX
				+ nnId;
		// curTxnFileLock.readLock().unlock();
		System.out.println("processingPath:" + processingPath);

		while (!done) {
			try {
				if (zk.exists(path, true) == null) {
					tmp = zk.create(path, state.getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).trim();
					done = tmp.equals(path);
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return done;
	}

	/**
	 * Get the state and path of the current txnIdFile
	 * 
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String[] getStateAndPath() {
		String ret[] = new String[3];// {state, path};
		String state = "NONE";
		String path = ROOT + PATH_SEPARATOR;
		String ops = "NON-OPS";
		String[] tmp = new String[2];
		boolean done = false;
		while (!done) {
			List<String> list;
			try {
				list = zk.getChildren(ROOT, false);
				if (list.size() > 0) {
					path += Collections.min(list).trim();
					state = new String(zk.getData(path, false, null)).trim();
					System.out.println("state:" + state + ", path:" + path);
					tmp = state.split(STATE_TXN_SEPARATOR);
					state = tmp[0];
					ops = tmp[1];
				} else {
					path += TXN_FILE_PREFIX + txnIdNumber;
				}
				processingPath = path;
				ret[0] = state;
				ret[1] = path;
				ret[2] = ops;
				// Thread.sleep(10000);
				done = true;
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}

	public void deleteReplyFile(long nnId) {
		boolean done = false;
		String[] stateAndPath = null;
		String state = null, path = null;
		while (!done) {
			try {
				stateAndPath = getStateAndPath();
				state = stateAndPath[0];
				// System.out.println("stateAndPath[1]:" + stateAndPath[1]);
				path = stateAndPath[1] + PATH_SEPARATOR + REPLY_FILE_PREFIX
						+ nnId;
				if (state.equals(TxnState.RELEASE_LOCK.text)) {
					zk.delete(path, -1);
					done = true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/********************* ConnectedWatcher ******************************/
	static class ConnectedWatcher implements Watcher {
		private CountDownLatch connectedLatch;

		ConnectedWatcher(CountDownLatch connectedLatch) {
			this.connectedLatch = connectedLatch;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getState() == KeeperState.SyncConnected) {
				connectedLatch.countDown();
			}
		}

	}

	class CoordinatorWatcher implements Watcher {
		private int hostMapSize;
		private ZooKeeper zk;
		private String path;
		private String opsType;

		CoordinatorWatcher(int hostMapSize, ZooKeeper zk, String path,
				String opsType) {
			this.hostMapSize = hostMapSize;
			this.zk = zk;
			this.path = path;
			this.opsType = opsType;
		}

		@Override
		public void process(WatchedEvent event) {
			boolean done = false;
			String stat = null;
			int replyCount = 0;
			switch (event.getType()) {
			case None:
				System.out.println("None..." + event.getPath());
				break;
			case NodeCreated:
				done = false;
				while (!done) {
					System.out.println("Event.getPath:" + event.getPath());
					try {
						if (done) {
							Thread.sleep(1000);
						}
						// if (zk.exists(path, false) != null) {
						System.out.println("0");
						stat = new String(zk.getData(path, true, null));
						System.out.println("0.1");
						replyCount = zk.getChildren(path, true).size();
						System.out.println("1");
						if (!allReplyFileCreated) {
							allReplyFileCreated = (stat
									.equals(TxnState.RELEASE_LOCK.text))
									|| (stat.equals(TxnState.PREPARE_LOCK.text) && (replyCount == hostMapSize));
							if (allReplyFileCreated
									&& stat.equals(TxnState.PREPARE_LOCK.text)) {
								System.out.println("2");
								updateTxnFileState(TxnState.RELEASE_LOCK.text);
							}
							System.out.println("1111Event.getPath:");
							stat = new String(zk.getData(path, true, null));
							replyCount = zk.getChildren(path, true).size();
							allReplyFileDeleted = stat
									.equals(TxnState.RELEASE_LOCK.text)
									&& (replyCount == 1);
						}
						System.out.println("222  Event.getPath:");
						if (!allReplyFileDeleted) {
							allReplyFileDeleted = stat
									.equals(TxnState.RELEASE_LOCK.text)
									&& (replyCount == 1);
							if (allReplyFileDeleted && (replyCount > 1)) {
								deleteTxnFile();
							}
						}
						System.out.println("allReplyFileCreated:"
								+ allReplyFileCreated
								+ ", allReplyFileDeleted:"
								+ allReplyFileDeleted);
						done = true;
					} catch (KeeperException e) {
						// /callBack.createTxnIdKEHandler(ops);
						System.out.println("1111 allReplyFileCreated:"
								+ allReplyFileCreated
								+ ", allReplyFileDeleted:"
								+ allReplyFileDeleted);

					} catch (InterruptedException e) {
						// callBack.createTxnIdIEHandler(ops);
						System.out.println("2222 allReplyFileCreated:"
								+ allReplyFileCreated
								+ ", allReplyFileDeleted:"
								+ allReplyFileDeleted);

					}
				}
				break;
			case NodeDeleted:
				System.out.println("NodeDeleted..." + event.getPath());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			case NodeChildrenChanged:
				System.out.println("NodeChildrenChanged..." + event.getPath());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			case NodeDataChanged:
				System.out.println("NodeDataChanged..." + event.getPath());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			}
		}
	}

	class NamenodeWatcher implements Watcher {

		private String nnId;
		private ZooKeeper zk;
		private String path;

		NamenodeWatcher(String nnId, ZooKeeper zk, String path) {
			this.nnId = nnId;
			this.zk = zk;
			this.path = path;
		}

		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			boolean done = false;
			String stat;
			switch (event.getType()) {
			case None:
			case NodeDeleted:
			case NodeChildrenChanged:
				break;
			case NodeCreated:
				while (!done) {
					try {
						stat = new String(zk.getData(path, false, null));
						if (stat.equals(TxnState.PREPARE_LOCK.text)) {
							createReplyFile(Long.parseLong(nnId));
						}
						done = true;
					} catch (KeeperException e) {
						// /callBack.createTxnIdKEHandler(ops);
					} catch (InterruptedException e) {
						// callBack.createTxnIdIEHandler(ops);
					}
				}
				break;
			case NodeDataChanged:
				done = false;
				while (!done) {
					try {
						stat = new String(zk.getData(path, false, null));
						if (stat.equals(TxnState.RELEASE_LOCK)) {
							deleteReplyFile(Long.parseLong(nnId));
						}
					} catch (KeeperException e) {
						// /callBack.createTxnIdKEHandler(ops);
					} catch (InterruptedException e) {
						// callBack.createTxnIdIEHandler(ops);
					}
				}
				break;
			}
		}
	}

}
