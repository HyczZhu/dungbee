package aStreamingHbaseIncrementalTransaction.transaction;

public class TransactionField {
	public static final byte[] DATA = ":data".getBytes();
	public static final byte[] WRITE = ":write".getBytes();
	public static final byte[] LOCK = ":lock".getBytes();
	public static final byte[] NOTIFY = ":notify".getBytes();
	public static final byte[] ACK_O = ":ack_O".getBytes();
	
	/***
	 * 将qualifier转化为带Lock字段到qualifier,例如：
	 * qualifier到内容是“sampleQualifer”，则返回
	 * “sampleQualifer:lock”
	 * @param qualifier
	 * @return
	 */
	public static byte[] convertToLockQualifier(byte[] qualifier)	{
		byte[] lockQual = new byte[qualifier.length + TransactionField.LOCK.length];
		System.arraycopy(qualifier, 0, lockQual, 0, qualifier.length);
		System.arraycopy(TransactionField.LOCK, 0, lockQual, qualifier.length, TransactionField.LOCK.length);
		return lockQual;
	}
	
	public static byte[] convertToWriteQualifier(byte[] qualifier)	{
		byte[] writeQual = new byte[qualifier.length + TransactionField.WRITE.length];
		System.arraycopy(qualifier, 0, writeQual, 0, qualifier.length);
		System.arraycopy(TransactionField.WRITE, 0, writeQual, qualifier.length, TransactionField.WRITE.length);
		return writeQual;
	}
	
	public static byte[] convertToDataQualifier(byte[] qualifier)	{
		byte[] dataQual = new byte[qualifier.length + TransactionField.DATA.length];
		System.arraycopy(qualifier, 0, dataQual, 0, qualifier.length);
		System.arraycopy(TransactionField.DATA, 0, dataQual, qualifier.length, TransactionField.DATA.length);
		return dataQual;
	}

	public static byte[] convertToNotifyQualifier(byte[] qualifier)	{
		byte[] notifyQual = new byte[qualifier.length + TransactionField.NOTIFY.length];
		System.arraycopy(qualifier, 0, notifyQual, 0, qualifier.length);
		System.arraycopy(TransactionField.NOTIFY, 0, notifyQual, qualifier.length, TransactionField.NOTIFY.length);
		return notifyQual;
	}
	
	public static byte[] convertToAckQualifier(byte[] qualifier)	{
		byte[] ackQual = new byte[qualifier.length + TransactionField.ACK_O.length];
		System.arraycopy(qualifier, 0, ackQual, 0, qualifier.length);
		System.arraycopy(TransactionField.ACK_O, 0, ackQual, qualifier.length, TransactionField.ACK_O.length);
		return ackQual;
	}
}
