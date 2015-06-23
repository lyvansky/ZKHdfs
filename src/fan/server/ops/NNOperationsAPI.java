package fan.server.ops;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

public class NNOperationsAPI {

	private ZookeeperOperations zkOps;

	public NNOperationsAPI(ZookeeperOperations zkOps) {
		this.zkOps = zkOps;
	}

	public boolean isTxnOperations(String ops) {
		boolean ret = true;
		// TODO Judges whether the ops is an txn?
		return ret;
	}

	public void doTxnLocal(String ops) {
		// TODO txn locally
		System.out.println("NNOperationsAPI::doTxnLocal");
		switch (ops) {
		case "mv":
			doRename("Lee", "Fan");
			break;
		case "del":
			// TODO delete operations
			break;
		default:
			System.out.println("Hello");
		}
	}

	public void sendTxnToCoordinator(String coordinator, String ops)
			throws UnknownHostException, IOException {
		// TODO send txn to the coordinator
		int servPort = 2281;
		byte[] data = ops.getBytes();
		Socket socket = new Socket(coordinator, servPort);
		System.out.println("Connected to NN... ");
		String str = "mv";// new String(data).toLowerCase();
		switch (str) {
		case "mv":
			/**
			 * 2. 通过套接字的输入输出流进行通信：一个Socket连接实例包括一个InputStream和一个OutputStream，
			 * 它们的用法同于其他Java输入输出流。
			 */
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			out.write(data);

			int totalBytesRcvd = 0;
			int bytesRcvd;

			while (totalBytesRcvd < data.length) {
				if ((bytesRcvd = in.read(data, totalBytesRcvd, data.length
						- totalBytesRcvd)) == -1) {
					throw new SocketException("Connection closed prematurely");
				}
				totalBytesRcvd += bytesRcvd;
			}
			System.out.println("Client: send end!");
			socket.close();
		}
	}

	public void doOperation(String ops) {
		// TODO operations
	}

	public void lock() {
		// TODO lock
	}

	public void unLock() {
		// TODO unlock
	}

	public void await(long time) {
		System.out.println("You have " + time + "s to do something....");
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(time + "s is over, Do true job now!");
		System.out.println();
	}

	public void doRename(String src, String dst) {
		// TODO rename
		System.out.println("NNOperationsAPI::doRename");
		boolean txnCreated = false, updated = false, allReplyReceived = false, allReplyFileDeleted = false;
		String ops = "mv src dst";
		txnCreated = zkOps.createTxnFile(ops);

		await(5000);
		System.out.println("Waiting For receiving all Reply File...");
		allReplyReceived = zkOps.hasReceivedAllReplyFile();
		System.out.println("All Reply File have Received!");
		System.out.println();
		if (allReplyReceived) {
			renameInt(src, dst);
			System.out.println();

			System.out.println("updateTxnFileState:"
					+ TxnState.RELEASE_LOCK.text + ", waiting...");
			updated = zkOps.updateTxnFileState(TxnState.RELEASE_LOCK.text);
			System.out.println("updated:" + updated);
			System.out.println();

			if (updated) {
				System.out.println("Waiting for all ReplyFile Deleted...");
				zkOps.allReplyFileDeleted();
				System.out.println("All ReplyFile have Deleted!");
				System.out.println();
				System.out.println("All ReplyFile have Deleted!");
				System.out.println();
				System.out.println("Begin to delete TxnFile...");
				zkOps.deleteTxnFile();
				System.out.println("TxnFile have Deleted!");
				System.out.println("Rename Transaction Completed!");

			}
		}
	}

	public void fromPrepareLockStep(String path, String ops) {
		boolean allReplyReceived = false;
		System.out.println("Waiting For receiving all Reply File...");
		allReplyReceived = zkOps.hasReceivedAllReplyFile();
		switch (ops) {
		case "MV":
			System.out.println("All Reply File have Received!");
			System.out.println();
			if (allReplyReceived) {
				fromDoingStep(path, ops);
			}
			break;
		case "DEL":
		case "OTHER":
			break;
		}
	}

	public void fromDoingStep(String path, String ops) {
		boolean updated = false;
		switch (ops) {
		case "MV":
			renameInt(ops, ops);
			System.out.println();

			System.out.println("updateTxnFileState:"
					+ TxnState.RELEASE_LOCK.text + ", waiting...");
			updated = zkOps.updateTxnFileState(TxnState.RELEASE_LOCK.text);
			System.out.println("updated:" + updated);
			System.out.println();
			if (updated) {
				fromReleaseLockStep(path, ops);
			}
			break;
		}

	}

	public void fromReleaseLockStep(String path, String ops) {

		System.out.println("Waiting for all ReplyFile Deleted...");
		zkOps.allReplyFileDeleted();
		System.out.println("All ReplyFile have Deleted!");
		System.out.println();
		System.out.println("All ReplyFile have Deleted!");
		System.out.println();
		System.out.println("Begin to delete TxnFile...");
		zkOps.deleteTxnFile();
		System.out.println("TxnFile have Deleted!");
		System.out.println("Rename Transaction Completed!");

	}

	private boolean renameInt(String src, String dst) {
		boolean ret = false;
		// TODO rename
		try {
			System.out.println("Do rename....");
			Thread.sleep(5000);
			System.out.println("Have Renamed!");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	public void doDelete(String src) {
		// TODO delete
	}
	
	public void createTxnFile(String ops){
		zkOps.createTxnFile(ops);
	}
}
