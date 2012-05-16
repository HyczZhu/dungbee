package aStreamingHbaseIncrementalTransaction.transaction;

public class TransactionException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3485732494129766405L;
	
	public TransactionException(String exceptionInfo)	{
		super(exceptionInfo);
	}
}
