package aStreamingHbaseIncrementalTransaction.transaction;

public class TransactionField {
	public static final byte[] DATA = "data".getBytes();
	public static final byte[] WRITE = "write".getBytes();
	public static final byte[] LOCK = "lock".getBytes();
	public static final byte[] NOTIFY = "notify".getBytes();
	public static final byte[] ACK_O = "ack_O".getBytes();
}
