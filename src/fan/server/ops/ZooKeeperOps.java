package fan.server.ops;

import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.test.ClientBase;

import fan.cfg.Cfg;

public class ZooKeeperOps {

	private final String Separator_Between_Diff_Host = "#";
	private final String Separator_Between_NNID_IP = "@";
	private final String HOST_MAP_DIR = "/mapdir";
	private String coordinator = Cfg.coordinator;
	
	public ZooKeeper connect(String host, Watcher wh) {
		boolean done = false;
		ZooKeeper zk = null;
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
		return zk;
	}
	
	public void registerToZK(String nnHostIp, String nnId, ZooKeeper zk, Map<String,String> hostMap) {
		boolean done = false;
		String path;
		String hostStr, tmp;
		String[] hosts, record;
		String nnId_ip = nnId + Separator_Between_NNID_IP + nnHostIp;

		while (!done) {
			try {
				if (zk.exists(HOST_MAP_DIR, false) == null) {
					path = zk.create(HOST_MAP_DIR, nnId_ip.getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					if (path.equals(HOST_MAP_DIR)) {
						hostMap.put(nnId, nnHostIp);
						done = true;
						coordinator = getCoordinator();
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
					coordinator = getCoordinator();
					done = true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public String getCoordinator() {
		//coordinator = "127.0.53.53";
		return coordinator;
	}
}
