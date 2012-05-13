package aStreamingHbaseIncrementalTransaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class TransactionOperation {
	private static final Log LOG = LogFactory.getLog(TransactionOperation.class);
	/***
	 * 当前事务到一个操作，操作成功返回true，失败返回false
	 * @return
	 */
	public abstract boolean operation();
	
	/***
	 * 如果当前事务失败，需要执行undo操作，把operation中执行到变化恢复
	 * @return
	 */
	public abstract boolean undo() throws UndoErrorException;
}
