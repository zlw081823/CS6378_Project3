import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

public class Client {

	public static void main(String[] args) {
		// Get clientID first
		String clientHostname = null;
		try {
			clientHostname = Inet4Address.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		final int clientID = Integer.parseInt(clientHostname.split("\\.")[0].split("c")[1]) - 25; // 1 - 7
		
		// Create a local file to keep track of message exchange information and time elapse
		createFile(clientID);
		System.out.println("Log file <client" + clientID + "> is created!");
		
		// Initiate the clients		
		setupInitiator(clientID);	// 1 - 7
		System.out.println("Setup finished...");	
		
		// Send request and receive ack
		int seqNum = 1;
		Random r = new Random();
		while (seqNum <= 41) {
			int serverID = r.nextInt(3) + 1;
			System.out.println("Send request to server <" + serverID + "> ...");
			
			// send request to server
			try {
				Socket sendSocket = new Socket("dc" + (serverID + 22) + ".utdallas.edu", 6666);
				ObjectOutputStream out = new ObjectOutputStream(sendSocket.getOutputStream());
				Message msgOut = new Message(clientID, clientHostname, seqNum, "request");
				out.writeObject(msgOut);
				out.close();
				sendSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// receive ack from lead server
			try {
				ServerSocket listenvSocket = new ServerSocket(6666);
				Socket recvSocket = listenvSocket.accept();
				ObjectInputStream in = new ObjectInputStream(recvSocket.getInputStream());
				Message msgIn = (Message) in.readObject();
				if (msgIn.getMsgType().equals("ack")) {	// Including the 41st ack - as terminate message
					System.out.println("Receve <ACK> from server[" + serverID + "]");
					seqNum ++;
				} else {
					System.out.println("Receive Wrong Massage from Server[" + serverID + "]: <" + msgIn.getMsgType() + ">!!");
					break;
				}
				in.close();
				recvSocket.close();
				listenvSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void createFile (int clientID) {
		String fileDir = "/tmp/user/java/client" + clientID;
		File file = new File(fileDir);
		
		if (file.exists()) {
			System.out.println("File <client" + clientID + "> already exists!");
			System.out.println("Erasing the file ...");
			FileWriter writer;
			try {
				writer = new FileWriter(file, false);
				writer.write("");
				writer.close();	
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			File parentFile = file.getParentFile();
			parentFile.mkdirs();
			try {
				file.createNewFile();
				System.out.println("File <client" + clientID + "> is created!");
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Creating file failed!");
			}
		}		
	}
	
	public static int setupInitiator(int clientID) {

		int sendCnt = 1;
		while (sendCnt < clientID) {
			String serverHostname = "dc" + (25 + sendCnt) + ".utdallas.edu";
			try {
				Socket sendSocket  = new Socket(serverHostname, 6666);
				ObjectOutputStream out = new ObjectOutputStream(sendSocket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(sendSocket.getInputStream());
				out.writeObject(new Message());
				if(in.readObject() != null) {
					System.out.println("Send to synchronize client<" + sendCnt + "> ... Ready");
					sendCnt ++;
				}
				
				out.close();
				in.close();
				sendSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}				
		}
		
		int recvCnt = 0;
		try {
			ServerSocket c1Socket = new ServerSocket(6666);
			while (recvCnt < 7 - clientID) {
				Socket acceptedSocket = c1Socket.accept();
				ObjectOutputStream out = new ObjectOutputStream(acceptedSocket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(acceptedSocket.getInputStream());
				if (in.readObject() != null) {	// Just read sth to indicate it is set up
					System.out.println("Received initiate from <" + acceptedSocket.getRemoteSocketAddress() + ">");
					recvCnt++;
				}
				out.writeObject(new Message());
				out.close();
				in.close();
				acceptedSocket.close();
			}
			c1Socket.close();		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return clientID;
	}
	
	public static void write1Line2File(int clientID, String log) {
		File file = new File("/tmp/user/java/client" + clientID);
		
		if (file.exists()) {
			try {
				FileWriter writer = new FileWriter(file, true);
				writer.write(log);
				writer.write(System.getProperty("line.separator"));	// Insert new line!
				writer.close();				
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Cannot open the file writer!");
			}
		}	
	}

}
