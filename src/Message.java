import java.io.Serializable;

public class Message implements Serializable{

	/**
	 * Default version UID
	 */
	private static final long serialVersionUID = 1L;

	private int senderID;
	private String senderHostName;
	private int seqNum;
	private String msgType;
	private boolean isFromServer;
	private int[] msgCnt = new int[6]; 
	
	public Message(int senderID, String senderHostName, int seqNum, String msgType) {
		this.senderID = senderID;
		this.senderHostName = senderHostName;
		this.seqNum = seqNum;
		this.msgType = msgType;
		this.isFromServer = false;
	}
	
	public Message() {
		// TODO Auto-generated constructor stub
		super();
	}
	
	public int getSenderID () {
		return senderID;
	}
	
	public String getSenderHostName () {
		return senderHostName;
	}
	
	public int getSeqNum () {
		return seqNum;
	}
	
	public void setMsgType (String msgType) {
		this.msgType = msgType;
	}
	
	public String getMsgType () {
		return msgType;
	}
	
	public void setIsFromServer () {
		isFromServer = true;
	}
	
	public boolean getIsFromServer () {
		return isFromServer;
	}
	
	public void setMsgCnt (int[] msgCnt) {
		System.arraycopy(msgCnt, 0, this.msgCnt, 0, 6);
	}
	
	public int[] getMsgCnt () {
		return msgCnt;
	}
}
