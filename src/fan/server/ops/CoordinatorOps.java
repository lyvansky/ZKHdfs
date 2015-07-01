package fan.server.ops;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import fan.cfg.Cfg;

public class CoordinatorOps {

	private final int BUFSIZE = Cfg.BUFSIZE;
	private final String stateOpsSeparator = Cfg.STATE_OPS_SEPARATOR;
	private final String pathSeparator = Cfg.PATH_SEPARATOR;

	private String txnPrefix = Cfg.TXN_FILE_PREFIX;

	private int txnCount = 0;

	private static Integer mutex = new Integer(-1);
	private ZooKeeper zk;
	private String root;// the transaction directory. E.g. "/txn"
	private int nnCount;
	private Boolean allReplyFileCreated = false, allReplyFileDeleted = false,
			doTxn = false;

	volatile String curTxnFile = new String();
	volatile String createPath = new String();
	// volatile String watcherPath = new String();

	private volatile TxnState state;
	private volatile String ops;

	private CommonOperations co = new CommonOperations();

	Watcher wh = new Watcher() {
		synchronized public void process(WatchedEvent event) {
			synchronized (mutex) {
				mutex.notify();
				// System.out.println("process....");
			}
		}
	};

	public CoordinatorOps(Integer mutex, ZooKeeper zk, String root, int nnCount) {
		this.mutex = mutex;
		this.zk = zk;
		this.root = root;
		this.nnCount = nnCount;
		updateCurTxnFile();
		updateNextTxnFileName();
	}

	public void doTxnRequest() {
		while (doTxn) {
			doTxn = false;
		}
	}

	public void doTxn(String doTxnFile) {
		try {
			// boolean equal = TxnState.DO_TXN.text.equals(new
			// String(zk.getData(curTxnFile, false,
			// null)).split(stateOpsSeparator)[0]);
			// System.out.println("doTxn:" + doTxn + ", equal:" + equal);
			// System.out.println("doTxn-->curTxnFile:" + curTxnFile + "|||len:"
			// + curTxnFile.length());
			// doTxnFile = getTxnFile();
			// if (doTxn
			// || (TxnState.DO_TXN.text
			// .equals(new String(zk.getData(doTxnFile, false,
			// null)).split(stateOpsSeparator)[0]))) {
			Cfg.killTime("Coordinator", "DoingTxn", false);
			System.out.println("Begin to do transaction....");
			TimeUnit.MILLISECONDS.sleep(5000);
			System.out.println("ops:" + ops);
			System.out.println("Transacation " + doTxnFile + " has finished!");
			if (zk.exists(doTxnFile, false) != null) {
				ops = new String(zk.getData(doTxnFile, false, null))
						.split(stateOpsSeparator)[1];
				byte[] data = (TxnState.RELEASE_LOCK.text + stateOpsSeparator + ops)
						.getBytes();
				while (zk.setData(doTxnFile, data, -1) == null)
					;
			}
			doTxn = false;
			// }
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Runnable coordinatorWatcherDaemon = new Runnable() {
		// Integer mutex, ZooKeeper zk, String root, int nnCount
		// TxnState state;
		String tmp[] = new String[2];

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				synchronized (mutex) {
					try {
						if (zk != null && (zk.exists(root, false) != null)) {
							List<String> list = zk.getChildren(root, false);
							if (list.size() > 0) {
								curTxnFile = getCurTxnFile();
								if (curTxnFile != null
										&& (zk.exists(curTxnFile, false) != null)) {

									tmp = new String(zk.getData(curTxnFile,
											false, null))
											.split(stateOpsSeparator);
									state = TxnState.valueOf(tmp[0]);
									ops = tmp[1];
									System.out.println("curTxnFile:"
											+ curTxnFile + ", " + "state:"
											+ state + ", ops:" + ops
											+ ", nnCount:" + nnCount
											+ ", wait for 3s......");
									TimeUnit.MILLISECONDS.sleep(3000);
									switch (state) {
									case PREPARE_LOCK:
										if (zk.getChildren(curTxnFile, wh)
												.size() == (nnCount - 1)) {// Re-register
											// the
											// wh
											System.out
													.println("Set state to Do_Txn....");
											byte[] data = (TxnState.DO_TXN.text
													+ stateOpsSeparator + ops)
													.getBytes();
											Cfg.killTime("Coordinator",
													"BeforeDoTxn", true);
											zk.setData(curTxnFile, data, -1);
											// allReplyFileCreated = true;
											doTxn = true;
											// doRelease = false;
											System.out
													.println("Set Do_Txn have done!");
										}
										mutex.wait();
										break;
									case DO_TXN:
										// doTxn = true;
										// allReplyFileCreated = false;
										// doRelease = false;
										zk.getChildren(curTxnFile, wh);// Re-register
																		// the
																		// wh
										mutex.wait();
										break;
									case RELEASE_LOCK:
										System.out.println("RELEASE_LOCK....");

										/**
										 * wait for all NN have deleted the
										 * reply file, and set a watcher on path
										 */
										if (zk.getChildren(curTxnFile, wh)
												.size() <= 0) {
											System.out.println("Deleting....");
											Cfg.killTime("Coordinator",
													"DeleteCurTxnFile", false);
											zk.delete(curTxnFile, -1);
											if (zk.getChildren(root, false)
													.size() <= 0) {
												updateCurTxnFile();
												mutex.wait();
											}
											// curTxnFile =
											// co.getCurrentWatcherPathForCoordinator(zk);//
											// change
											// the
											// Watcher
											// Path
											doTxn = false;
											// allReplyFileCreated = false;
											// allReplyFileDeleted = true;
											System.out.println("Deleted!");
										}
										mutex.wait();
										break;
									case EXEC_DELETE:
										break;
									case EXEC_RENAME:
										break;
									default:
										break;
									}
								}
							}
						}
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	};

	/** listen is a thread that used to server for all client requests */
	public Runnable coordinatorListenDaemon = new Runnable() {

		@Override
		public void run() {
			String ops;
			int servPort = 2281;// Integer.parseInt(args[0]);
			ServerSocket servSock = null;
			try {
				servSock = new ServerSocket(servPort);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Receiving Client TxnRequest......");
			int recvMsgSize;
			byte[] receiveBuf = new byte[BUFSIZE];
			while (true) {
				try {
					Socket clntSock = null;
					clntSock = servSock.accept();
					InputStream in = clntSock.getInputStream();
					OutputStream out = clntSock.getOutputStream();

					while ((recvMsgSize = in.read(receiveBuf)) != -1) {
						out.write(receiveBuf, 0, recvMsgSize);
					}
					ops = new String(receiveBuf);
					ops = ops.trim();
					System.out.println("ops:" + ops);
					createTxnFile(ops);
					clntSock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	};

	private void createTxnFile(String ops) {
		boolean done = false;
		String txnFile = null;
		String tmp;
		byte[] data = (TxnState.PREPARE_LOCK.text + stateOpsSeparator + ops)
				.getBytes();
		while (!done) {
			try {
				// if (txnFile == null) {
				txnFile = getCreatePath();
				// }
				tmp = zk.create(txnFile, data, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				// if(zk.getChildren(txnFile, watch))
				System.out.println("tmp:" + tmp + ", txnFile:" + txnFile);
				if (tmp.equals(txnFile)) {
					synchronized (mutex) {
						updateNextTxnFileName();
						if (zk.getChildren(root, false).size() == 1) {
							mutex.notify();
						}
					}
					done = true;
				} else {
					done = false;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public synchronized int setAndGetTxnCount(int count) {
		if (count > 0) {
			txnCount = count + 1;
		} else {
			txnCount += 1;
		}
		return txnCount;
	}

	public String getDoTxnFile() {
		String state;
		synchronized (mutex) {
			List<String> list;
			try {
				list = zk.getChildren(root, false);
				if (list.size() > 0) {
					String min = Collections.min(list);
					curTxnFile = root + pathSeparator + min;
					state = new String(zk.getData(curTxnFile, false, null))
							.split(stateOpsSeparator)[0];
					if (TxnState.DO_TXN.text.equals(state)) {
						return curTxnFile;
					} else if (TxnState.RELEASE_LOCK.text.equals(state)) {
						if (list.remove(min) && (list.size() > 0)) {
							min = Collections.min(list);
							return root + pathSeparator + min;
						}
					}
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}

	public String getCurTxnFile() {
		updateCurTxnFile();
		return curTxnFile;
	}

	public String getCreatePath() {
		return createPath;
	}

	public synchronized void updateNextTxnFileName() {

		synchronized (createPath) {
			String path = null;
			List<String> list;
			try {
				list = zk.getChildren(root, false);
				if (list.size() > 0) {
					txnCount = Integer.parseInt(Collections.max(list).split(
							txnPrefix)[1].trim()) + 1;
					path = root + pathSeparator + txnPrefix + txnCount;
				} else {
					path = root + pathSeparator + txnPrefix + (++txnCount);
				}
				createPath = path;
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public synchronized void updateCurTxnFile() {
		List<String> list;
		try {
			list = zk.getChildren(root, false);
			if (list.size() > 0) {
				curTxnFile = root + pathSeparator + Collections.min(list);
			} else
				curTxnFile = null;
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Runnable doTxnDaemon = new Runnable() {

		@Override
		public void run() {
			while (true) {
				// System.out.println("doTxnDaemon-->curTxnFIle:" +curTxnFile);
				String doTxnFile = getDoTxnFile();
				if (doTxnFile != null && (doTxnFile.length() > 0)) {
					Cfg.killTime("Coordinator", "doTxn", true);
					doTxn(doTxnFile);
				} else {
					try {
						TimeUnit.MILLISECONDS.sleep(1000);
					} catch (InterruptedException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
				}
			}
		}

	};
}
