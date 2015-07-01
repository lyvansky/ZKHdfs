package fan.server.namenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;

import fan.cfg.Cfg;
import fan.server.ops.CoordinatorOps;
import fan.server.ops.NamenodeOps;
import fan.server.ops.ZooKeeperOps;

public class NameNode {

	private final String root = Cfg.ROOT;

	private boolean isCoordinator = false;
	private String coordinator;// the coordiantor ip
	
	private String zkHost = Cfg.zkHost;
	private String host = Cfg.server;
	
	/** contains all NN, order by nnId */
	private Map<String, String> hostMap = new TreeMap<String, String>();

	private String nnId = "1";
	private static Integer mutex = new Integer(-1);

	private ZooKeeper zk;

	ZooKeeperOps zko = new ZooKeeperOps();


	/**
	 * initiate the Namenode
	 */
	public void initNN() {
		//zko.connect(zk, zkHost, null);
		isCoordinator = host.equals(zko.getCoordinator());
		zk = zko.connect(zkHost, null);
		if(zk ==null){
			System.out.println("zk=null");
		}
		zko.registerToZK(host, nnId, zk, hostMap);
	}

	public void start() {

	}



	// Integer mutex, ZooKeeper zk, String root, int nnCount
	public static void main(String[] args) {
		
		NameNode namenode = new NameNode();		
		
		namenode.initNN();
		
		/**block watcherDaemon && doActionDaemon*/
		if (namenode.isCoordinator) {
			/**
			 * watcherDaemon
			 */
			CoordinatorOps coordinator = new CoordinatorOps(namenode.mutex,
					namenode.zk, namenode.root, namenode.hostMap.size());

			Thread watcher = new Thread(coordinator.coordinatorWatcherDaemon);// watcher
																				// thread
			Thread listen = new Thread(coordinator.coordinatorListenDaemon);
			Thread doTxnOps = new Thread(coordinator.doTxnDaemon);
			watcher.setDaemon(true);
			watcher.start();
			listen.start();
			doTxnOps.start();
			
		}else{
			//(Integer mutex, ZooKeeper zk, String root, String nnId)
			NamenodeOps nn = new NamenodeOps(namenode.zk, namenode.root, namenode.nnId);
			Thread watcher = new Thread(nn.nnWatcherDaemon);
			Thread listenAndDoOps = new Thread(nn.listenAndDoOps);
			
			watcher.setDaemon(true);
			watcher.start();
			listenAndDoOps.start();
			
		}
	}

}
