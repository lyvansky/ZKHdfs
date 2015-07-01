package old.fan.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import old.fan.cfg.Cfg;

public class Client {

	private static final String server = Cfg.nnAddress;

	public static void main(String[] args) throws UnknownHostException,
			IOException, InterruptedException {

		byte[] data = "mv".getBytes();

		int servPort = 2281;
		Socket socket = new Socket(server, servPort);
		System.out.println("Connected to NN... ");
		String str = "mv";// new String(data).toLowerCase();
		switch (str) {
		case "mv":
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

	public void printUsage() {
		System.out.println("mv  [srcs]   [dst]");
	}
}
