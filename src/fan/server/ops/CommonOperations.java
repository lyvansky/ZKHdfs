package fan.server.ops;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import fan.cfg.Cfg;

public class CommonOperations {

	private String root = Cfg.ROOT;
	private String pathSeparator = Cfg.PATH_SEPARATOR;

	private String txnPrefix = Cfg.TXN_FILE_PREFIX;

	private int txnCount = 0;
	
	public String watchPath;

	private Integer mutex = new Integer(-1);

	/**
	 * the minimal child entry under directory root is the current transaction
	 * file name. The function of this method is to get the new transaction
	 * file.
	 * 
	 * @param zk
	 * @param root
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getCurrentWatcherPathForCoordinator(ZooKeeper zk)
			throws KeeperException, InterruptedException {
		synchronized (mutex) {
			String path = null;
			List<String> list = zk.getChildren(root, false);
			if (list.size() > 0) {
				path = root + pathSeparator + Collections.min(list).trim();
				
			} else {
				mutex.wait();
			}
			watchPath = path;
			return path;
		}
	}

	public String getNextTxnFileName(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		// synchronized (mutex) {
		String path = null;
		int count = 0;
		List<String> list = zk.getChildren(root, false);
		if (list.size() > 0) {
			count = Integer.parseInt(Collections.max(list).split(txnPrefix)[1]
					.trim()) + 1;
			count = setAndGetTxnCount(count);
			path = root + pathSeparator + txnPrefix + count;
		} else {
			path = root + pathSeparator + txnPrefix + setAndGetTxnCount(0);
			mutex.notify();
		}
		return path;
		// }
	}

	public synchronized int setAndGetTxnCount(int count) {
		if (count > 0) {
			txnCount = count + 1;
		} else {
			txnCount += 1;
		}
		return txnCount;
	}

	public boolean isTxnOperation(String ops){
		boolean ret = true;
		//TODO judge whether ops is a transaction operation
		return ret;
	}
	
}
