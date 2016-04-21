import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ServerHandler {
	private volatile BlockingQueue<Message> commitReqQ = new ArrayBlockingQueue<>(10);
	private volatile int[] agreeCnt = new int[7];	// worst case scenario, all 7 request on one server
	private volatile int ackCnt = 0;	
	private volatile int terminateCnt = 0;

	public void requestHandler(int serverID, Message msgIn, Socket recvSocket) {
		if (msgIn.getIsFromServer()) {
			// Message from server
			if (msgIn.getSeqNum() == 41) {
				// Only LEADER will receive this message from server
				terminateCnt ++;
				if (terminateCnt == 7) {
					msgIn.setMsgType("terminate");
					forwardMsg2AllServers(msgIn, serverID);
					forwardMsg2AllClients(msgIn);
					sendMsg2Server(msgIn, "terminate", "dc23.utdallas.edu");
				}
			} else {
				// Regular process
				String targetHostname = recvSocket.getInetAddress().getHostName();	// Should work
				sendMsg2Server(msgIn, "agreed", targetHostname);
				System.out.println("Receive message from server... Send AGREED back...");
			}
		} else {
			// Message from client
			if (msgIn.getSeqNum() == 41) {
				if (serverID == 1) {
					// LEADER
					terminateCnt ++;
					if (terminateCnt == 7) {
						msgIn.setMsgType("terminate");
						msgIn.setIsFromServer();
						forwardMsg2AllServers(msgIn, serverID);
						forwardMsg2AllClients(msgIn);
						sendMsg2Server(msgIn, "terminate", "dc23.utdallas.edu");
					}
				} else {
					// Not LEADER - send this terminate message to LEADER
					msgIn.setIsFromServer();
					sendMsg2Server(msgIn, "request", "dc23.utdallas.edu");					
				}
			} else {
				// Regular process
				msgIn.setIsFromServer();
				forwardMsg2AllServers(msgIn, serverID);
				System.out.println("Receive message from client... Forward it to server...");			
			}
		}
	}
	
	public void agreedHandler(int serverID, Message msgIn) {
		agreeCnt[msgIn.getSenderID() - 1] ++;
		if (agreeCnt[msgIn.getSenderID() - 1] == 2) {
			agreeCnt[msgIn.getSenderID() - 1] = 0;
			if (serverID == 1) {	// Leader
				// Just like processing a new coming COMMIT_REQUEST
				if(commitReqQ.isEmpty()) {
					commitReqQ.add(msgIn);
					write1Line2File(msgIn.getSenderID(), "<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
					msgIn.setMsgType("commit");
					forwardMsg2AllServers(msgIn, serverID);
					System.out.println("Leader received a client's REQ and committed the WRITE... Forward COMMIT to other servers...");
				} else {
					commitReqQ.add(msgIn);
					System.out.println("Previous COMMIT_REQ hasn't finish... Wait in the queue...");
				}
			} else {
				sendMsg2Server(msgIn, "commit_request", "dc23.utdallas.edu");
				System.out.println("Send COMMIT_REQUEST to leader!");						
			}
		}
	}
	
	public void commit_requestHandler(int serverID, Message msgIn) {
		// Only leader will receive this message!
		if(commitReqQ.isEmpty()) {
			commitReqQ.add(msgIn);
			write1Line2File(msgIn.getSenderID(), "<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
			msgIn.setMsgType("commit");
			forwardMsg2AllServers(msgIn, serverID);
			System.out.println("Leader committed the WRITE... Forward COMMIT to other servers...");
		} else {
			commitReqQ.add(msgIn);
			System.out.println("Previous COMMIT_REQ hasn't finish... Wait in the queue...");
		}
	}
	
	public void commitHandler(Message msgIn) {
		// Only non-leader will receive this message!
		write1Line2File(msgIn.getSenderID(), "<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
		sendMsg2Server(msgIn, "ack", "dc23.utdallas.edu");
		System.out.println("Committed the WRITE... Send ACK to leader...");		
	}
	
	public void ackHandler(int serverID, Message msgIn) throws InterruptedException {
		// Only leader will receive this message
		ackCnt ++;
		if (ackCnt == 2) {
			ackCnt = 0;
			commitReqQ.take();
			sendMsg2Server(msgIn, "ack", msgIn.getSenderHostName());
			System.out.println("Received all ACKs... Send ACK to client...");
			//process next COMMIT_REQUEST
			if (commitReqQ.isEmpty()){
				// wait for new COMMIT_REQUESTS / REQUESTS
				// Do nothing
			} else {
				write1Line2File(msgIn.getSenderID(), "<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
				Message msg = commitReqQ.peek();
				msg.setMsgType("commit");
				forwardMsg2AllServers(msg, serverID);
				System.out.println("Leader retrieved a new client REQ and committed the WRITE... Forward COMMIT to other servers...");
			}
		}
	}
	
	public static void forwardMsg2AllServers (Message msgIn, int serverID) {	
		
		for (int sID = 1; sID <= 3; sID ++) {
			if (sID != serverID) {
				try {
					Socket s2sSocket = new Socket("dc" + (sID + 22) + ".utdallas.edu", 6666);
					ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
					Message msgS2SOut = msgIn;
					msgS2SOut.setIsFromServer();	// Mark the msg(basically REQUEST) is sent from server
					out.writeObject(msgS2SOut);			
					out.close();
					s2sSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Cannot connect to server[" + sID + "]");
				}
			}
		}
	}
	
	public static void forwardMsg2AllClients (Message msg) {	
		
		for (int sID = 1; sID <= 7; sID ++) {
			try {
				Socket s2sSocket = new Socket("dc" + (sID + 25) + ".utdallas.edu", 6666);
				ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
				Message msgS2SOut = msg;
				msgS2SOut.setIsFromServer();	
				out.writeObject(msgS2SOut);			
				out.close();
				s2sSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Cannot connect to server[" + sID + "]");
			}
		}
	}
	
	public static void sendMsg2Server (Message msgIn, String msgType, String targetHostname) {
		try {
			Socket s2sSocket = new Socket(targetHostname, 6666);
			ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
			Message msgOut = msgIn;
			msgIn.setMsgType(msgType);	// Change the msgType, but maintain the original request info
			out.writeObject(msgOut);			
			out.close();
			s2sSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
