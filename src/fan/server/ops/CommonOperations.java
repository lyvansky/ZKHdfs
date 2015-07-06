package fan.server.ops;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	static Configuration conf1 = new Configuration(),
			conf2 = new Configuration();
	static FileSystem hdfs1, hdfs2;

	static {
		conf1.set("fs.default.name", "hdfs://172.16.19.22:9000");
		conf2.set("fs.default.name", "hdfs://172.16.253.178:9000");
		try {
			hdfs1 = FileSystem.get(conf1);
			hdfs2 = FileSystem.get(conf2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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

	public boolean isTxnOperation(String ops) {
		boolean ret = true;
		// TODO judge whether ops is a transaction operation
		return ret;
	}

	public boolean doDelete(String[] srcs) {
		// TODO
		// ops = srcs[0]
		// path = srcs[1,..,len-1]
		boolean ret = false;
		for (int i = 1; i < srcs.length; i++) {
			while (!ret) {
				try {
					ret = delete(hdfs1, new Path(srcs[i]), true);
				} catch (IllegalArgumentException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return ret;
	}

	public boolean doRename(String[] operations, ZooKeeper zk, String doTxnFile) {
		// ops = paths[0]
		// srcs = paths[1,...,len-2]
		// dst = paths[len -1]
		boolean ret = false, created = false, deleted = false;
		if (operations.length != 3) {
			System.out.println("Illegal parameter! E.g. <ops src dst>");
			return false;
		}
		Path src = new Path(operations[1]);
		Path dst = new Path(operations[2]);
		String sPrefix = src.toString();
		String dPrefix = dst.toString();
		while (!ret) {
			try {
				// if (!created) {
				ret = rename(src, dst, sPrefix, dPrefix, zk, doTxnFile);
				// created = true;
				// deleted = false;
				// }
				// if(!deleted){
				// ret = delete(hdfs1, src, true);
				// deleted = true;
				// }

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}

	boolean rename(Path src, Path dst, String sPrefix, String dPrefix,
			ZooKeeper zk, String doTxnFile) throws FileNotFoundException,
			IOException {
		boolean ret = false, hasSubState = false;
		String tmps, tmpd;
		FileStatus files[] = hdfs1.listStatus(src);
		hasSubState = hasSubState(zk, doTxnFile);
		System.out.println("sPrefix:" + sPrefix + ", dPrefix:" + dPrefix);
		if (!hasSubState) {
			if (hdfs2.exists(dst)) {
				/** delete all entries under dst folder and finally delete dst */
				ret = delete(hdfs2, dst, true);
			}
			updateOpsState(zk, doTxnFile, "hasDeleted");
			//Cfg.killTime("Coordinator", "DO_TXN:Deleted", true);
		}
		if (files.length <= 0) {
			if (hdfs1.getFileStatus(src).isDirectory()) {
				ret = hdfs2.mkdirs(dst);
			} else {
				ret = hdfs2.createNewFile(dst);
			}
			ret = hdfs1.delete(src, false);
			return ret;
		} else {
			for (FileStatus file : files) {
				if (file.isFile()) {
					tmps = file.getPath().toUri().getRawPath();
					tmpd = tmps.replace(sPrefix, dPrefix);
					System.out.println("1.file:" + tmps + ", dst:" + dst
							+ ", ret:" + ret);
					ret = hdfs2.createNewFile(new Path(tmpd));
					ret = hdfs1.delete(new Path(tmps), false);
				} else {
					tmps = file.getPath().toUri().getRawPath();
					Path csrc = new Path(tmps);
					Path cdst = new Path(tmps.replace(sPrefix, dPrefix));
					// System.out.println("2.file:" + csrc + ", dst:" + dst
					// + ", ret:" + ret);
					ret = rename(csrc, cdst, sPrefix, dPrefix, zk, doTxnFile);
				}
				// Cfg.killTime("Coordinator", "Rename Ops", true);
			}
			if (hdfs1.getFileStatus(src).isDirectory()) {
				System.out.println("11.src:" + src + ", dst:" + dst);
				ret = hdfs2.mkdirs(dst);
			} else {
				System.out.println("22.dst:" + dst);
				ret = hdfs2.createNewFile(dst);
			}
			ret = hdfs1.delete(src, false);
		}
		return ret;
	}

	boolean delete(FileSystem hdfs, Path path, boolean recursive)
			throws FileNotFoundException, IOException {
		boolean ret = false;
		FileStatus files[] = hdfs.listStatus(path);
		if (files.length <= 0) {
			ret = hdfs.delete(path, false);
		} else {
			if (recursive) {
				for (FileStatus file : files) {
					// hdfs = getFileSystem(file.getPath());
					if (file.isFile()) {
						System.out.println("file delete:" + path.getName());
						ret = hdfs.delete(file.getPath(), false);
					} else {
						Path cpath = file.getPath();
						ret = delete(hdfs, cpath, true);
					}
					// Cfg.killTime("Coordinator", "Delete Ops", true);
				}
			} else {
				return false;
			}
			ret = hdfs.delete(path, false);
		}
		return ret;
	}

	FileSystem getFileSystem(Path path) {
		FileSystem hdfs = null;
		return hdfs;
	}

	public void updateOpsState(ZooKeeper zk, String doTxnFile, String state) {
		String tmp, newState;
		String[] components;
		String[] opsComponents;
		boolean done = false;
		while (!done) {
			if (zk != null) {
				try {
					tmp = new String(zk.getData(doTxnFile, false, null));
					components = tmp.split(Cfg.STATE_OPS_SEPARATOR);
					opsComponents = components[1]
							.split(Cfg.OPS_PARAMETER_SEPARATOR);
					// newState = components[0] + Cfg.STATE_OPS_SEPARATOR +
					// opsComponents
					opsComponents[0] += Cfg.OPS_SUBSTATE_SEPARATOR + state;
					state = components[0] + Cfg.STATE_OPS_SEPARATOR;
					for (int i = 0; i < opsComponents.length; i++) {
						state += opsComponents[i] + Cfg.OPS_PARAMETER_SEPARATOR;
					}
					state = state.substring(0, state.length() - 1);
					zk.setData(doTxnFile, state.getBytes(), -1);
					done = true;
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	boolean hasSubState(ZooKeeper zk, String doTxnFile) {
		boolean done = false;
		String state;
		while (!done) {
			try {
				if (zk != null && (zk.exists(doTxnFile, false) != null)) {
					state = new String(zk.getData(doTxnFile, false, null))
							.split(Cfg.STATE_OPS_SEPARATOR)[1]
							.split(Cfg.OPS_PARAMETER_SEPARATOR)[0];
					String[] ss = state.split(Cfg.OPS_SUBSTATE_SEPARATOR);
					if (ss.length > 1) {
						return true;
					} else {
						return false;
					}
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	public String getOpsState(ZooKeeper zk, String doTxnFile) {
		boolean done = false;
		String state;
		while (!done) {
			try {
				if (zk != null && (zk.exists(doTxnFile, false) != null)) {
					state = new String(zk.getData(doTxnFile, false, null))
							.split(Cfg.STATE_OPS_SEPARATOR)[1]
							.split(Cfg.OPS_PARAMETER_SEPARATOR)[0];
					String[] ss = state.split(Cfg.OPS_SUBSTATE_SEPARATOR);
					if (ss.length > 1) {
						return ss[1];
					} else {
						return null;
					}
				} else {
					return null;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
}
