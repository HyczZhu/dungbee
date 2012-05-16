package aStreamingHbaseIncrementalTransaction.client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

import aStreamingHbaseIncrementalTransaction.transaction.TransactionField;

public class TPut extends Put {
	/***
	 * 
	 * @param row
	 * @param ts
	 * @param rowLock
	 */
	public TPut(byte[] row , long ts , RowLock rowLock)	{
		super(row , ts , rowLock);
	}
	
	/***
	 * 
	 * @param family
	 * @param ts
	 * @param value
	 * @return
	 */
	public TPut add(byte[] family , long ts , byte[] value)	{
		super.add(family, TransactionField.DATA, ts, value);
		return this;
	}
}
