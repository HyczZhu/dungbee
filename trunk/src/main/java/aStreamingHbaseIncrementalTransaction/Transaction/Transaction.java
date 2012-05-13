package aStreamingHbaseIncrementalTransaction.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Transaction {
	private static final Log LOG = LogFactory.getLog(Transaction.class);
	private List<TransactionOperation> transactionList;
	private Stack<TransactionOperation> undoStack;

	Transaction()	{
		transactionList = new ArrayList<TransactionOperation>();
		undoStack = new Stack<TransactionOperation>(); 
	}
	
	/***
	 * 为事务添加一个操作，添加到操作将按被添加顺序执行
	 * @param operation
	 */
	public void addOperation(TransactionOperation operation)	{
		transactionList.add(operation);
	}
	
	/***
	 * 移除当前事务中到一个操作，如果该操作不在当前事务操作列表中，返回false
	 * @param operation
	 * @return
	 */
	public boolean removeOperation(TransactionOperation operation)	{
		return transactionList.remove(operation);
	}
	
	/***
	 * 提交事务，若事务成功，返回true，若提交失败，把之前到操作进行回滚，返回false
	 * @return
	 */
	public boolean commit()	{
		synchronized(this)	{
			for(TransactionOperation op : transactionList)	{
				undoStack.push(op);
				if(op.operation() == false)	{
					if(!rollback())	{
						LOG.warn("ROLL BACK ERROR!");
					}
					return false;
				}
			}
			return true;
		}
	}
	
	/***
	 * 
	 * @return
	 */
	public boolean rollback()	{
		boolean ret = true;
		TransactionOperation op;
		
		synchronized(undoStack)	{
			while(!undoStack.empty())	{
				op = undoStack.pop();
				try {
					if(op.undo() == false)	{
						LOG.warn(op.getClass().getName() + " UNDO ERROR!");
						ret = false;
					}
				} catch (UndoErrorException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return ret;
	}
}






