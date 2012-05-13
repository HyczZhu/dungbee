package aStreamingHbaseIncrementalTransaction.client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

import aStreamHbaseIncrementalTransaction.Transaction.TransactionField;

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
	 * @param qualifier
	 * @param ts
	 * @param value
	 * @return
	 */
	public TPut add(byte[] family , byte[] qualifier , long ts , byte[] value)	{
		byte[] dataQualifier = 
				new byte[qualifier.length + TransactionField._DATA.length];
		
		System.arraycopy(qualifier, 0, dataQualifier, 0, qualifier.length);
		System.arraycopy(TransactionField._DATA, 0, dataQualifier,
				qualifier.length, TransactionField._DATA.length);
		super.add(family, dataQualifier, ts, value);
		
		return this;
	}
}
