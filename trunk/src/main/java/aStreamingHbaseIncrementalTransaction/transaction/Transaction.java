package aStreamingHbaseIncrementalTransaction.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import aStreamingHbaseIncrementalTransaction.multirowTransaction.RowTransaction;

public class Transaction {
	private static final Log LOG = LogFactory.getLog(Transaction.class);
	private List<TransactionWrite> writeList;
	long startTimestamp;	//当前事务开始时间
	
	public Transaction()	{
		writeList = new ArrayList<TransactionWrite>();
		startTimestamp = System.currentTimeMillis();
	}
	
	/***
	 * 添加一个 {@link TransactionWrite} 操作到当前事务的写列表之中
	 * @param w
	 * @throws TransactionException
	 */
	public void set(TransactionWrite w) throws TransactionException	{
		if(w.getTableName() == null || w.getRow() == null || w.getCol() == null)	{
			LOG.error("aSHIT.transaction.Transaction#set(): " +
					  "tableName or row or column could NOT be null");
			throw new TransactionException("TransactionWrite row and column could NOT be null");
		}
		writeList.add(w);
	}
	
	/***
	 * 
	 * @param row
	 * @param col
	 * @return 返回指定存储单元的最新值，如果不存在，则返回null
	 * @throws IOException
	 * @throws TransactionException 
	 */
	public byte[] get(String table , byte[] row , byte[] col)
			throws IOException, TransactionException	{
		
		int waitCnt = 0;
		while(true)	{
			//检查将要操作的存储单元是否被上锁了
			//如果被上锁了，则等待锁释放
			if(checkExistByTimeRange(table , row , col , TransactionField.LOCK , 0 , startTimestamp))	{
				waitCnt++;
				if(waitCnt > 10)	{
					LOG.warn("aSHIT.transaction.Transaction#get(): Wait Lock Time Out.");
					LOG.warn(" row = " + new String(row) + " # col = " + new String(col));
					throw new TransactionException(
							" row: " + new String(row) + 
							" col: " + new String(col) +
							" was locked too LONG!!! ");
				}
				tryToCleanupLock(row , col);
				continue;
			}
			
			Result latestWrite = getByTimeRange(table , row , col , 
					TransactionField.WRITE , 0 , startTimestamp);
			if(latestWrite.isEmpty())	{
				return null;	//write字段未被写入时间戳(说明没有data字段没有数据)
			}
			//获取最近一次在write字段里面写入到时间戳，并根据该时间戳获取最近的数据
			//注意，write字段存储的是上一次提交的事务的开始时间戳，该时间戳用于找到正确的data字段
			//而write字段本身的时间戳，是上一次事务提交时的时间戳
			long maxStamp = 0L;
			for(KeyValue kv : latestWrite.raw())	{
				LOG.debug(" row: " + new String(kv.getRow()) + 
						  " col: " + new String(kv.getFamily()) +
						  " qualifier: " + new String(kv.getQualifier()) +
						  " last Tr commit timestamp: " + kv.getTimestamp() + 
						  " last Tr start timestamp: " + new String(kv.getValue()));
				
				String val = new String(kv.getValue());
				long timestamp = Long.parseLong(val);
				if(timestamp > maxStamp)	{
					maxStamp = timestamp;
				}
			}
			Result dataResult = getByTimestamp(table , row , col , TransactionField.DATA , maxStamp);
			return dataResult.getValue(col, TransactionField.DATA);
		}
	}
	
	/***
	 * 判断在指定的时间戳范围内，指定的存储单元中是否存放有数据
	 * @param row
	 * @param col
	 * @param qualifier
	 * @param minStamp
	 * @param maxStamp
	 * @return
	 * @throws IOException
	 */
	private boolean checkExistByTimeRange(String table , byte[] row , byte[] col , 
			byte[] qualifier ,  long minStamp , long maxStamp) throws IOException	{
		
		Result r = getByTimeRange(table , row , col , qualifier , minStamp , maxStamp);
		return !r.isEmpty();
	}
	
	/***
	 * 这里仅仅是简单睡眠500ms，等待锁释放。该函数的功能等以后有时间再增加
	 * @param row
	 * @param col
	 */
	private void tryToCleanupLock(byte[] row , byte[] col)	{
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/***
	 * 根据特定的时间戳区间查询结果
	 * @param row
	 * @param col
	 * @param qualifier
	 * @param minStamp
	 * @param maxStamp
	 * @return
	 * @throws IOException
	 */
	private Result getByTimeRange(String table , byte[] row , byte[] col , 
			byte[] qualifier , long minStamp , long maxStamp) throws IOException	{

		Get get = new Get(row);
		get.setTimeRange(minStamp, maxStamp);
		RowTransaction rowTransaction = new RowTransaction(table);
		return rowTransaction.lockAndGet(get);
	}
	
	/***
	 * 根据特定的时间戳查询结果
	 * @param row
	 * @param col
	 * @param qualifier
	 * @param timestamp
	 * @return
	 * @throws IOException
	 */
	private Result getByTimestamp(String table , byte[] row , byte[] col , 
			byte[] qualifier , long timestamp)
		throws IOException	{
		return getByTimeRange(table , row , col , qualifier , timestamp , timestamp+1);
	}
	
	/***
	 * 
	 * @param w
	 * @param primary
	 * @param overWrite 如果w的data字段中存在有数据，是否写覆盖
	 * @return
	 * @throws IOException
	 */
	private boolean preWrite(TransactionWrite w , TransactionWrite primary , boolean overWrite) 
			throws IOException	{
		
		//检查当前事务开始时间点以后是否已有Write字段写入，若被写入，说明当前事务已经过期，返回false
		if(checkExistByTimeRange(w.getTableName(), w.getRow(), w.getCol(),
				TransactionField.WRITE, startTimestamp, Long.MAX_VALUE))	{
			return false;
		}
		//检查该存储单元是否被锁住，如果被锁住，则事务失败
		if(checkExistByTimeRange(w.getTableName(), w.getRow(), w.getCol(),
				TransactionField.LOCK, 0, Long.MAX_VALUE))	{
			return false;
		}
		//将w的数据写入到data字段中，并写入w的主锁的位置
		RowTransaction rt = new RowTransaction();
		rt.lockAndPut(w.getTableName(),  w.getRow(), w.getCol(),
				TransactionField.DATA, w.getVal(), startTimestamp, overWrite);
		String primaryPosition = new String(
				new String(primary.getRow()) + "#" + new String(primary.getCol()));
		rt.lockAndPut(w.getTableName(), w.getRow(), w.getCol(),
				TransactionField.LOCK, primaryPosition.getBytes(), startTimestamp, overWrite);
		return true;
	}
	
	/***
	 * 提交事务
	 * @return 事务成功返回true，失败返回false
	 * @throws TransactionException
	 * @throws IOException
	 */
	public boolean commit() throws TransactionException, IOException	{
		if(writeList.isEmpty())	{
			throw new TransactionException("No TransactionWrite");
		}
		TransactionWrite prime = writeList.get(0);
		//primary TransactionWrite 的 preWrite 操作
		if(!preWrite(prime, prime, true))	{
			return false;
		}
		//secondaries TransactionWrite 的 preWrite 操作
		for(int index = 1 ; index != writeList.size() ; ++index)	{
			if(!preWrite(writeList.get(index) , prime , true))	{
				return false;
			}
		}
		
		RowTransaction rowTransaction = new RowTransaction(prime.getTableName());
		//检测主锁是否存在，若不存在，则说明事务被终止了
		if(!checkExistByTimeRange(prime.getTableName(), prime.getRow(), prime.getCol(),
				TransactionField.LOCK, startTimestamp, startTimestamp+1))	{
			return false;
		}
		//在Write字段中写入事务开始时间，事务提交时间为该字段到时间戳
		long commitTimestamp = System.currentTimeMillis();
		byte[] startTs = (startTimestamp + "").getBytes();
		rowTransaction.lockAndPut(prime.getTableName(), prime.getRow(), prime.getCol(),
				TransactionField.WRITE, startTs, commitTimestamp, true);
		//将Lock字段擦除
		rowTransaction.lockAndDelete(prime.getTableName(), prime.getRow(), prime.getCol(),
				TransactionField.LOCK, commitTimestamp);
		
		for(int index = 1 ; index != writeList.size() ; ++index)	{
			TransactionWrite w = writeList.get(index);
			rowTransaction.lockAndPut(w.getTableName(), w.getRow(), w.getCol(),
					TransactionField.WRITE, startTs, commitTimestamp, true);
			rowTransaction.lockAndDelete(w.getTableName(), w.getRow(), w.getCol(),
					TransactionField.LOCK, commitTimestamp);
		}
		return true;
	}
}






