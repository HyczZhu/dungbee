package aStreamingHbaseIncrementalTransaction.multirowTransaction;

import java.io.IOException;

import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.RowMutations;

import aStreamingHbaseIncrementalTransaction.example.TestBase;

public class RowTransaction {

	private static final int MAX_COLUMN = Integer.MAX_VALUE;
	
	private static HTablePool pool;
	
	static {
		pool = TestBase.getTablePool();
	}
	
	public static HTable getHTable(String tableName){
		return  (HTable) pool.getTable(tableName);
	}
	
	//use lock to put and get
	
	/***
	 * lock and get data from a column family
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("finally")
	Result lockAndGet(String tableName, String rowName, String familyName) throws IOException{
		HTable table = getHTable(tableName);
		Result result=null;
		
		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);
		
		RowLock rowLock = table.lockRow(row);
		System.out.println("row lock id: " + rowLock.getLockId());
		//long time = System.currentTimeMillis();
		
		try {
			Get get = new Get(row, rowLock);
			get.addFamily(family);
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+ (System.currentTimeMillis()-time)+ "ms" );
			try {
				System.out.println("releasing lock ...");
				// System.out.println("row lock id: "+rowLock.getLockId());
				table.unlockRow(rowLock);
				// System.out.println("after unlock");
			} catch (final UnknownRowLockException e) {
				System.out
						.println("the lock is already discarded by the server");
				e.printStackTrace();
			}
			table.close();
			return result;
		}
	}
	
	/***
	 * lock and get data from a column 
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("finally")
	Result lockAndGet(String tableName, String rowName, String familyName,
			String columnName) throws IOException{
		HTable table = getHTable(tableName);
		Result result=null;
		
		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);
		byte[] column = Bytes.toBytes(columnName);

		RowLock rowLock = table.lockRow(row);
		System.out.println("row lock id: " + rowLock.getLockId());
		//long time = System.currentTimeMillis();
		
		try {
			Get get = new Get(row, rowLock);
			get.addColumn(family, column);
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+ (System.currentTimeMillis()-time)+ "ms" );
			try {
				System.out.println("releasing lock ...");
				// System.out.println("row lock id: "+rowLock.getLockId());
				table.unlockRow(rowLock);
				// System.out.println("after unlock");
			} catch (final UnknownRowLockException e) {
				System.out
						.println("the lock is already discarded by the server");
				e.printStackTrace();
			}
			table.close();
			return result;
		}
	}
	
	/***
	 * use row lock to put data, if data already exists, over write it
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @param value
	 * @throws IOException
	 */
	public void lockAndPut(String tableName, String rowName, String familyName,
			String columnName, String value) throws IOException {
		lockAndPut(tableName, rowName, familyName, columnName, value, true);
	}

	/***
	 * use row lock to put data
	 * 
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @param value
	 * @param doOverwrite
	 * @throws IOException
	 */
	public void lockAndPut(String tableName, String rowName, String familyName,
			String columnName, String value, boolean doOverwrite)
			throws IOException {
		HTable table = getHTable(tableName);

		// just for test
		// randomize columnName
		// columnName = columnName + (int) (Math.random() * MAX_COLUMN);

		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);
		byte[] column = Bytes.toBytes(columnName);
		
		RowLock rowLock = table.lockRow(row);
		System.out.println("row lock id: " + rowLock.getLockId());
		//long time = System.currentTimeMillis();
		
		try {
			// Check if the record exists
			if (!doOverwrite) {
				Get get = new Get(row, rowLock);
				Result result = table.get(get);
				if (!result.getColumn(family, column).isEmpty()) {
					System.out.println("record exists, quit");
					throw new Exception("This column already exists.");
				}
			}

			// Row does not exist yet, create it
			Put p = new Put(row, rowLock);
			p.add(family, column, Bytes.toBytes(value));
			System.out.println("putting " + tableName + "/" + rowName + "/"
					+ familyName + "/" + columnName + ":" + value);
			table.put(p);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+ (System.currentTimeMillis()-time)+ "ms" );
			try {
				System.out.println("releasing lock ...");
				// System.out.println("row lock id: "+rowLock.getLockId());
				table.unlockRow(rowLock);
				// System.out.println("after unlock");
			} catch (UnknownRowLockException e) {
				System.out
						.println("the lock is already discarded by the server");
				e.printStackTrace();
			}

			table.close();
		}
	}
	

	//use RowMutations to do a row transaction
	
	public static void doRowMutation(){
		RowMutations arm;
	}
	
/***
 * incrementColumnValue

public long incrementColumnValue(byte[] row,
                                 byte[] family,
                                 byte[] qualifier,
                                 long amount,
                                 boolean writeToWAL)
                          throws IOException
Atomically increments a column value. If the column value already exists and is not a big-endian long, this could throw an exception. If the column value does not yet exist it is initialized to amount and written to the specified column.
Setting writeToWAL to false means that in a fail scenario, you will lose any increments that have not been flushed.

Specified by:
incrementColumnValue in interface HTableInterface
Parameters:
row - The row that contains the cell to increment.
family - The column family of the cell to increment.
qualifier - The column qualifier of the cell to increment.
amount - The amount to increment the cell with (or decrement, if the amount is negative).
writeToWAL - if true, the operation will be applied to the Write Ahead Log (WAL). This makes the operation slower but safer, as if the call returns successfully, it is guaranteed that the increment will be safely persisted. When set to false, the call may return successfully before the increment is safely persisted, so it's possible that the increment be lost in the event of a failure happening before the operation gets persisted.
Returns:
The new value, post increment.
Throws:
IOException - if a remote or network exception occurs.
 */
	
/***
 * checkAndPut

public boolean checkAndPut(byte[] row,
                           byte[] family,
                           byte[] qualifier,
                           byte[] value,
                           Put put)
                    throws IOException
Atomically checks if a row/family/qualifier value matches the expected value. If it does, it adds the put. If the passed value is null, the check is for the lack of column (ie: non-existance)
Specified by:
checkAndPut in interface HTableInterface
Parameters:
row - to check
family - column family to check
qualifier - column qualifier to check
value - the expected value
put - data to put if check succeeds
Returns:
true if the new put was executed, false otherwise
Throws:
IOException - e
 */
	
/***
 * checkAndDelete

public boolean checkAndDelete(byte[] row,
                              byte[] family,
                              byte[] qualifier,
                              byte[] value,
                              Delete delete)
                       throws IOException
Atomically checks if a row/family/qualifier value matches the expected value. If it does, it adds the delete. If the passed value is null, the check is for the lack of column (ie: non-existance)
Specified by:
checkAndDelete in interface HTableInterface
Parameters:
row - to check
family - column family to check
qualifier - column qualifier to check
value - the expected value
delete - data to delete if check succeeds
Returns:
true if the new delete was executed, false otherwise
Throws:
IOException - e
 */
}
