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
	private volatile int[] msgCnt = new int[6];
	//private Lock lock = new ReentrantLock();

	public synchronized void requestHandler(int serverID, Message msgIn, Socket recvSocket) {
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
				//this.countMsg(msgIn);	// this.method VS. static method??
				System.out.println("Receive REQUEST from server... Send AGREED back...");
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
				System.out.println("Receive REQUEST from client... Forward it to other servers...");			
			}
		}
	}
	
	public synchronized void agreedHandler(int serverID, Message msgIn) {
		agreeCnt[msgIn.getSenderID() - 1] ++;
		if (agreeCnt[msgIn.getSenderID() - 1] == 2) {
			agreeCnt[msgIn.getSenderID() - 1] = 0;
			if (serverID == 1) {	// Leader
				// Just like processing a new coming COMMIT_REQUEST
				if(commitReqQ.isEmpty()) {
					commitReqQ.add(msgIn);
					write1Line2File("<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
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
	
	public synchronized void commit_requestHandler(int serverID, Message msgIn) {
		// Only leader will receive this message!
		if(commitReqQ.isEmpty()) {
			commitReqQ.add(msgIn);
			write1Line2File("<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
			msgIn.setMsgType("commit");
			forwardMsg2AllServers(msgIn, serverID);
			System.out.println("Leader committed the WRITE... Forward COMMIT to other servers...");
		} else {
			commitReqQ.add(msgIn);
			System.out.println("Previous COMMIT_REQ hasn't finish... Wait in the queue...");
		}
	}
	
	public synchronized void commitHandler(Message msgIn) {
		// Only non-leader will receive this message!
		write1Line2File("<" + msgIn.getSenderID() + ", " + msgIn.getSeqNum() + ", " + msgIn.getSenderHostName() + ">");
		sendMsg2Server(msgIn, "ack", "dc23.utdallas.edu");
		System.out.println("Committed the WRITE... Send ACK to leader...");		
	}
	
	public synchronized void ackHandler(int serverID, Message msgIn) throws InterruptedException {
		// Only leader will receive this message
		ackCnt ++;
		if (ackCnt == 2) {
			ackCnt = 0;
			commitReqQ.take();
			msgIn.setMsgCnt(msgCnt);
			for (int i  = 0; i < msgCnt.length; i ++)
				msgCnt[i] = 0;	//Clear the count
			sendMsg2Server(msgIn, "ack", msgIn.getSenderHostName());	// Method name is quite confusing, since the msg is sent to client
			System.out.println("Received all ACKs... Send ACK to client...");
			//process next COMMIT_REQUEST
			if (commitReqQ.isEmpty()){
				// wait for new COMMIT_REQUESTS / REQUESTS
				// Do nothing
			} else {
				Message msg = commitReqQ.peek();
				write1Line2File("<" + msg.getSenderID() + ", " + msg.getSeqNum() + ", " + msg.getSenderHostName() + ">");
				msg.setMsgType("commit");
				forwardMsg2AllServers(msg, serverID);
				System.out.println("Leader retrieved a new client REQ and committed the WRITE... Forward COMMIT to other servers...");
			}
		}
	}
	
	public void forwardMsg2AllServers (Message msgIn, int serverID) {	
		
		for (int sID = 1; sID <= 3; sID ++) {
			if (sID != serverID) {
				try {
					Socket s2sSocket = new Socket("dc" + (sID + 22) + ".utdallas.edu", 6666);
					ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
					Message msgS2SOut = msgIn;
					msgS2SOut.setIsFromServer();	// Mark the msg(basically REQUEST) is sent from server
					this.countMsg(msgS2SOut);
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
	
	public void forwardMsg2AllClients (Message msg) {	
		
		for (int sID = 1; sID <= 7; sID ++) {
			try {
				Socket s2sSocket = new Socket("dc" + (sID + 25) + ".utdallas.edu", 6666);
				ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
				Message msgS2SOut = msg;
				msgS2SOut.setIsFromServer();
				this.countMsg(msgS2SOut);
				out.writeObject(msgS2SOut);			
				out.close();
				s2sSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Cannot connect to server[" + sID + "]");
			}
		}
	}
	
	public void sendMsg2Server (Message msgIn, String msgType, String targetHostname) {
		try {
			Socket s2sSocket = new Socket(targetHostname, 6666);
			ObjectOutputStream out = new ObjectOutputStream(s2sSocket.getOutputStream());					
			Message msgOut = msgIn;
			msgIn.setMsgType(msgType);	// Change the msgType, but maintain the original request info
			this.countMsg(msgOut);
			out.writeObject(msgOut);			
			out.close();
			s2sSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write1Line2File(String log) {
		File file = new File("/tmp/user/java/server");
		
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
	
	public void countMsg (Message msg) {
		if (msg.getMsgType().equals("request")) {
			this.msgCnt[0] ++;
		} else if (msg.getMsgType().equals("agreed")) {
			this.msgCnt[1] ++;
		} else if (msg.getMsgType().equals("commit_request")) {
			this.msgCnt[2] ++;
		} else if (msg.getMsgType().equals("commit")) {
			this.msgCnt[3] ++;
		} else if (msg.getMsgType().equals("ack")) {
			this.msgCnt[4] ++;
		} else if (msg.getMsgType().equals("terminate")) {
			this.msgCnt[5] ++;
		}  
	}
}
