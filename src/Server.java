import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class Server {
	public volatile static boolean listeningFlg = true;
	public volatile static boolean shutdownFlg = false;
	
	public static void main(String[] args) throws IOException {
		// Get server ID first
		String serverHostname = null;
		try {
			serverHostname = Inet4Address.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		final int serverID = Integer.parseInt(serverHostname.split("\\.")[0].split("c")[1]) - 22; // 1 - 3
	
		// Create a log file
		createFile();
		System.out.println("Log file <server> is created!");		
		
		// Create a handler for all incoming messages
		ServerHandler handler = new ServerHandler();
		
		// Start the termination detection thread
		new Thread(new Runnable() {
			public void run() {
				while (listeningFlg);
				try {
					Thread.sleep(1000);
					shutdownFlg = true;
					new Socket("dc" + (serverID + 22) + ".utdallas.edu", 6666).close();;
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
		
		// Listening to incoming message
		ServerSocket serverSocket = new ServerSocket(6666);		
		boolean listening = true;
		while(listening) {
			Socket clientSocket = serverSocket.accept();
			
			if (shutdownFlg == false) {
				Thread clientSocketHandlerThread = new Thread(new Runnable() {
					public void run() {
						// Read the incoming message
						try {
							ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
							Message msgIn = (Message) in.readObject();
							if(msgIn.getIsFromServer()) {
								System.out.println("Receive a message: <" + msgIn.getMsgType() + ">, from server <" + msgIn.getSenderID() + ">");													
							} else {
								System.out.println("Receive a message: <" + msgIn.getMsgType() + ">, from client <" + msgIn.getSenderID() + ">");						
							}
							in.close();
							clientSocket.close();
							
							if(msgIn.getMsgType().equals("request")) {
								handler.requestHandler(serverID, msgIn, clientSocket);
							} else if (msgIn.getMsgType().equals("agreed")) {
								handler.agreedHandler(serverID, msgIn);
							} else if (msgIn.getMsgType().equals("commit_request")) {
								handler.commit_requestHandler(serverID, msgIn);
							} else if (msgIn.getMsgType().equals("commit")) {
								handler.commitHandler(msgIn);
							} else if (msgIn.getMsgType().equals("ack")) {
								handler.ackHandler(serverID, msgIn);
							} else {	
								// Terminate
								//Thread.sleep(1000);
								listeningFlg = false;
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				});
				clientSocketHandlerThread.start();				
			} else {
				break;
			}
		}
		serverSocket.close();
		System.out.println("Server <" + serverID + "> is closed!");
	}
	
	public static void createFile () {
		String fileDir = "/tmp/user/java/server";
		File file = new File(fileDir);
		
		if (file.exists()) {
			System.out.println("File <server> already exists!");
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
				System.out.println("File <server> is created!");
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Creating file failed!");
			}
		}		
	}	
	
}
