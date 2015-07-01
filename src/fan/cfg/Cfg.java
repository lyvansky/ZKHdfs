package fan.cfg;

import java.util.concurrent.TimeUnit;

public class Cfg {
	public static final String zkHost = "172.16.253.178";// the zk server

	public static final String server = "172.16.253.178";// the namenode ip
	public static final int servPort = 2281;

	public static final String coordinator = "172.16.253.178";

	public static final int BUFSIZE = 32;

	public static final String ROOT = "/txn";

	public static final String TXN_FILE_PREFIX = "txn_";
	public static final String PATH_SEPARATOR = "/";

	public static final String STATE_OPS_SEPARATOR = "#";

	public static final String nnId = "1";
	public static final String REPLY_PREFIX = "reply_";

	public static final String HOST_MAP_DIR = "/mapdir";

	public static final long COORDINATOR_SLEEP_INTERVAL = 10000;
	public static final long NN_SLEEP_INTERVAL = 10000;

	public static void coordinatorSleep() {
		System.out
				.println("Coordinator will sleep"
						+ COORDINATOR_SLEEP_INTERVAL
						+ "ms after this sentence display, you can kill the Coordinator now...");
		try {
			TimeUnit.MILLISECONDS.sleep(COORDINATOR_SLEEP_INTERVAL);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Kill Time Over!");
	}

	public static void nnSleep() {
		System.out
				.println("NameNode will sleep"
						+ NN_SLEEP_INTERVAL
						+ "ms after this sentence display, you can kill the NameNode now...");
		try {
			TimeUnit.MILLISECONDS.sleep(NN_SLEEP_INTERVAL);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Kill Time Over!");
	}

	public static void killTime(String role, String state, boolean sleep) {
		if (sleep) {
			System.out.println("Kill " + role + " before " + state
					+ ", sleeping " + NN_SLEEP_INTERVAL
					+ "ms, you can kill now...");
			try {
				TimeUnit.MILLISECONDS.sleep(NN_SLEEP_INTERVAL);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Kill Time Over!");
		}
	}
}
