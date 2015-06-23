package fan.server.namenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.KeeperException;

import fan.cfg.Cfg;
import fan.server.ops.NNOperationsAPI;
import fan.server.ops.TxnState;
import fan.server.ops.ZookeeperOperations;

public class NameNode {

	static ZookeeperOperations operations = new ZookeeperOperations();
	NNOperationsAPI nnOps = new NNOperationsAPI(operations);

	private static final int BUFSIZE = 32;
	private boolean isCoordinator = false;

	private String coordinator;
	private String zkHost = Cfg.zkHost;
	private String host = Cfg.nnAddress;

	Map<String, String> hostMap = new TreeMap<String, String>();

	public void initNN() {
		System.out.println("zkHost:" + zkHost + ", host:" + host);
		hostMap = operations.initNN(zkHost, host);
		coordinator = operations.getAndSetCoordinator();
		System.out.println("coordinator:" + coordinator);
		isCoordinator = host.equals(coordinator) ? true : false;
	}

	public void createTxnOrDoOperations(String ops) {

		System.out.println("NameNode: isTxnOps:" + nnOps.isTxnOperations(ops)
				+ ", isCoordinator:" + isCoordinator);
		// nnOps = new NNOperationsAPI(operations);
		if (nnOps.isTxnOperations(ops)) {
			System.out.println("createTxnFile");
			nnOps.createTxnFile(ops);
		} else {
			nnOps.doOperation(ops);
		}
	}

	public void processOps(String ops) throws IOException,
			InterruptedException, KeeperException {

		System.out.println("NameNode: isTxnOps:" + nnOps.isTxnOperations(ops)
				+ ", isCoordinator:" + isCoordinator);
		// nnOps = new NNOperationsAPI(operations);
		if (nnOps.isTxnOperations(ops)) {
			if (isCoordinator) {
				nnOps.doTxnLocal(ops);
			} else {
				nnOps.sendTxnToCoordinator(coordinator, ops);
			}
		} else {
			nnOps.doOperation(ops);
		}
	}

	Runnable doTxnDaemon = new Runnable() {

		@Override
		public void run() {
			String[] statePathAndOps = new String[3];
			String state, path, ops;

			System.out.println("Do Transaction......");
			while (true) {
				statePathAndOps = operations.getStateAndPath();
				state = statePathAndOps[0];
				path = statePathAndOps[1];
				ops = statePathAndOps[2];
				ops = "mv";

				switch (state) {
				case "NONE":
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case "PREPARE_LOCK":
					nnOps.fromPrepareLockStep(path, ops.toUpperCase());
					break;
				case "DOING":
					nnOps.fromDoingStep(path, ops.toUpperCase());
					break;
				case "RELEASE_LOCK":
					nnOps.fromReleaseLockStep(path, ops.toUpperCase());
					break;
				}
			}
		}
	};

	Runnable listenDaemon = new Runnable() {

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
			System.out.println("startNN。。。。");
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
					createTxnOrDoOperations(ops);
					clntSock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	};
	Runnable zkDaemon = new Runnable() {
		@Override
		public void run() {
			System.out.println("Hello");
			String[] tmp;
			String state, path, cp = "null";
			String nnId = operations.getNNID();
			AtomicBoolean created = new AtomicBoolean(false);
			AtomicBoolean deleted = new AtomicBoolean(false);
			long i = 0;
			while (true) {
				try {
					tmp = operations.getStateAndPath();
					state = tmp[0];
					path = tmp[1];

					System.out.println("i:" + (i++) + ",  cp:" + cp
							+ ",   path:" + path + ", state:" + state
							+ ", created:" + created + ", deleted:" + deleted);
					if (created.get() && deleted.get()) {
						Thread.sleep(1000);
						created.set(false);
						deleted.set(false);
					}
					Thread.sleep(2000);

					// System.out.println("state:" + state);
					switch (state) {
					case "NONE":
						Thread.sleep(1000);
						break;
					case "PREPARE_LOCK":
						if (!created.get()) {
							created.set(true);
							operations.createReplyFile(Long.parseLong(nnId));
							deleted.set(false);
							// cp = path;
						}
						break;
					case "RELEASE_LOCK":
						if (!deleted.get()) {
							deleted.set(true);
							operations.deleteReplyFile(Long.parseLong(nnId));
							created.set(false);
							// cp = path;
						}
						break;
					}

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	};


	public static void main(String[] args) {
		final NameNode nn = new NameNode();
		nn.initNN();
		Thread listen = new Thread(nn.listenDaemon);
		listen.start();
		if (!nn.isCoordinator) {// NN is not the coordinator
			Thread daemon = new Thread(nn.zkDaemon);
			daemon.setDaemon(true);
			daemon.start();
		} else {// NN is the coordinator, to find whether is restart
			Thread doTxn = new Thread(nn.doTxnDaemon);
			doTxn.setDaemon(true);
			doTxn.start();
		}
	}
}
