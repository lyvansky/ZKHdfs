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

public class NamenodeOps {

	private final int BUFSIZE = Cfg.BUFSIZE;
	private final String stateOpsSeparator = Cfg.STATE_OPS_SEPARATOR;
	private final String replyPrefix = Cfg.REPLY_PREFIX;
	private final String pathSeparator = Cfg.PATH_SEPARATOR;

	private Integer mutex = new Integer(-1);
	private ZooKeeper zk;
	private String root;// the transaction directory. E.g. "/txn"
	private String nnId = Cfg.nnId;
	private Boolean created = false, deleted = false;

	volatile String curTxnFile;
	private String replyFile;

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

	public NamenodeOps(ZooKeeper zk, String root, String nnId) {
		this.zk = zk;
		this.root = root;
		this.nnId = nnId;
	}

	public Runnable listenAndDoOps = new Runnable() {

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
			System.out.println("Receiving Client Request......");
			int recvMsgSize;
			byte[] receiveBuf = new byte[BUFSIZE];
			while (true) {
				try {
					Socket clntSock = null;
					clntSock = servSock.accept();
					SocketAddress clientAddress = clntSock
							.getRemoteSocketAddress();
					System.out.println("Handling client at " + clientAddress);
					InputStream in = clntSock.getInputStream();
					OutputStream out = clntSock.getOutputStream();

					while ((recvMsgSize = in.read(receiveBuf)) != -1) {
						out.write(receiveBuf, 0, recvMsgSize);
					}
					ops = new String(receiveBuf);
					ops = ops.trim();
					System.out.println("ops:" + ops);
					process(ops);
					clntSock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	};

	public Runnable doOps = new Runnable() {

		@Override
		public void run() {
			// TODO

		}

	};

	public Runnable nnWatcherDaemon = new Runnable() {

		String tmp[] = new String[2];

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				synchronized (mutex) {
					try {
						curTxnFile = getTxnFile();
						if (zk != null && (curTxnFile != null)
								&& (zk.exists(curTxnFile, false) != null)) {
							replyFile = curTxnFile + Cfg.PATH_SEPARATOR
									+ replyPrefix + nnId;
							System.out.println("curTxnFile:" + curTxnFile);
							List<String> list = zk.getChildren(curTxnFile, false);
							String reply = replyPrefix + nnId;
							tmp = new String(
									zk.getData(curTxnFile, false, null))
									.split(stateOpsSeparator);
							state = TxnState.valueOf(tmp[0]);
							switch (state) {
							case PREPARE_LOCK:
								System.out.println("PrepareLock:" + (!list.contains(reply)));
								if (!list.contains(reply)) {
									Cfg.killTime("Namenode", "createReplyFile", false);
									createReplyFile(replyFile);
									zk.getData(curTxnFile, wh, null);
								}
								mutex.wait();
								break;
							case DO_TXN:
								zk.getData(curTxnFile, wh, null);// Re-register
																	// the wh
								mutex.wait();
								break;
							case RELEASE_LOCK:
								System.out.println("Release_Lock:" + (list.contains(reply)));
								
								Cfg.killTime("Namenode", "DeleteReplyFile", false);
								System.out.println("Delete ReplyFile....");

								if (list.contains(reply)) {
									deleteReplyFile(replyFile);
									zk.getData(curTxnFile, wh, null);
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
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	};

	public String getTxnFile() {
		synchronized (mutex) {
			List<String> list;
			try {
				list = zk.getChildren(root, false);
				if (list.size() > 0) {
					curTxnFile = root + pathSeparator + Collections.min(list);
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return curTxnFile;
		}
	}

	private void createReplyFile(String path) {
		boolean done = false;
		System.out.println("Create ReplyFile....");
		String tmp;
		while (!done) {
			try {
				if (zk.exists(replyFile, wh) == null) {
					deleted = false;
					tmp = zk.create(path, "LOCKED".getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("tmp:" + tmp + ", path:" + path
							+ ", tmp.equal(path):" + tmp.equals(path));
					if (tmp.equals(path)) {
						created = true;
						done = true;
					}
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO �Զ���ɵ� catch ��
				e.printStackTrace();
			}
		}
		System.out.println("ReplyFile has Created!");
	}

	private void deleteReplyFile(String path) {
		boolean done = false;
		System.out.println("Delete ReplyFile....");
		// String tmp;
		while (!done) {
			try {
				if (zk.exists(replyFile, wh) != null) {

					created = false;
					zk.delete(path, -1);
					deleted = true;
					done = true;
				}
			} catch (InterruptedException | KeeperException e) {
				// TODO �Զ���ɵ� catch ��
				e.printStackTrace();
			}
		}
		System.out.println("ReplyFile has Deleted!");
	}

	private void process(String ops) {
		System.out.println("Do Operations....");
		try {
			TimeUnit.MILLISECONDS.sleep(3000);
		} catch (InterruptedException e) {
			// TODO �Զ���ɵ� catch ��
			e.printStackTrace();
		}
		System.out.println("Non-transaction Operations have finished!");
	}
}
