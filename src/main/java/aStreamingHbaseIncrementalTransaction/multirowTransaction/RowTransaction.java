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

	public static HTable getHTable(String tableName) {
		return (HTable) pool.getTable(tableName);
	}

	private static String tableName;

	// use lock to put and get

	/***
	 * lock and get
	 * 
	 * @param get
	 * @return
	 * @throws IOException
	 */
	Result lockAndGet(Get get) throws IOException {
		HTable table = getHTable(get.getRow().toString());
		Result result = null;

		RowLock rowLock = table.lockRow(get.getRow());
		System.out.println("row lock id: " + rowLock.getLockId());
		// long time = System.currentTimeMillis();

		try {
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+
			// (System.currentTimeMillis()-time)+ "ms" );
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
		}
		return result;
	}

	/***
	 * lock and get data from a column family
	 * 
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @return
	 * @throws IOException
	 */
	Result lockAndGet(String tableName, String rowName, String familyName)
			throws IOException {
		HTable table = getHTable(tableName);
		Result result = null;

		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);

		RowLock rowLock = table.lockRow(row);
		System.out.println("row lock id: " + rowLock.getLockId());
		// long time = System.currentTimeMillis();

		try {
			Get get = new Get(row, rowLock);
			get.addFamily(family);
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+
			// (System.currentTimeMillis()-time)+ "ms" );
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
		}
		return result;
	}

	/***
	 * lock and get data from a column
	 * 
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @return
	 * @throws IOException
	 */
	Result lockAndGet(String tableName, String rowName, String familyName,
			String columnName) throws IOException {
		HTable table = getHTable(tableName);
		Result result = null;

		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);
		byte[] column = Bytes.toBytes(columnName);

		RowLock rowLock = table.lockRow(row);
		System.out.println("row lock id: " + rowLock.getLockId());
		// long time = System.currentTimeMillis();

		try {
			Get get = new Get(row, rowLock);
			get.addColumn(family, column);
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Wait time: "+
			// (System.currentTimeMillis()-time)+ "ms" );
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
		}
		return result;
	}

	/***
	 * use row lock to put data, if data already exists, over write it
	 * 
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
		// long time = System.currentTimeMillis();

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
			// System.out.println("Wait time: "+
			// (System.currentTimeMillis()-time)+ "ms" );
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

	// use RowMutations to do a row transaction

	public static void doRowMutation() {
		RowMutations arm;
	}

	// incrementColumnValue methods

	/***
	 * simple wrap of incrementColumnValue method from HTable.
	 * 
	 * @param table
	 * @param row
	 * @param family
	 * @param qualifier
	 * @param amount
	 * @return
	 * @throws IOException
	 */
	public long incrementColumnValue(byte[] table, byte[] row, byte[] family,
			byte[] qualifier, long amount) throws IOException {
		HTable htable = (HTable) pool.getTable(table);
		return htable.incrementColumnValue(row, family, qualifier, amount);
	}

	/***
	 * simple wrap of incrementColumnValue method from HTable.
	 * 
	 * @param table
	 * @param row
	 * @param family
	 * @param qualifier
	 * @param amount
	 * @param writeToWAL
	 * @return
	 * @throws IOException
	 */
	public long incrementColumnValue(byte[] table, byte[] row, byte[] family,
			byte[] qualifier, long amount, boolean writeToWAL)
			throws IOException {
		HTable htable = (HTable) pool.getTable(table);
		return htable.incrementColumnValue(row, family, qualifier, amount,
				writeToWAL);
	}

	/***
	 * simple wrap of checkAndPut method from HTable. if value of the specific
	 * column already exists, this method won't change the value. if the
	 * specific column doesn't exists, this method create this column and set
	 * value.
	 * 
	 * @param row
	 * @param family
	 * @param column
	 * @param value
	 * @throws IOException
	 */
	public boolean checkAndPut(byte[] table, byte[] row, byte[] family,
			byte[] column, byte[] value) throws IOException {
		HTable htable = (HTable) TestBase.getTablePool().getTable(table);
		Put put = new Put(row);
		put.add(family, column, value);
		boolean rt = false;

		try {
			rt = htable.checkAndPut(row, family, column, null, put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			htable.close();
		}
		return rt;
	}

	/***
	 * simple wrap of checkAndPut method from HTable. if value of the specific
	 * column already exists, this method won't change the value. if the
	 * specific column doesn't exists, this method create this column and set
	 * value.
	 * 
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @param value
	 * @throws IOException
	 */
	public boolean checkAndPut(String tableName, String rowName,
			String familyName, String columnName, String value)
			throws IOException {
		HTable table = (HTable) TestBase.getTablePool().getTable(
				Bytes.toBytes(tableName));
		byte[] row = Bytes.toBytes(rowName);
		byte[] family = Bytes.toBytes(familyName);
		byte[] column = Bytes.toBytes(columnName);
		Put put = new Put(row);
		put.add(family, column, Bytes.toBytes(value));
		boolean rt = false;

		try {
			rt = table.checkAndPut(row, family, column, null, put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
		return rt;
	}

	/***
	 * simple wrap of checkAndPut method from HTable.
	 * 
	 * @param table
	 * @param row
	 * @param family
	 * @param qualifier
	 * @param value
	 * @param put
	 * @return
	 * @throws IOException
	 */
	public boolean checkAndPut(byte[] table, byte[] row, byte[] family,
			byte[] qualifier, byte[] value, Put put) throws IOException {
		HTable htable = (HTable) pool.getTable(table);
		return htable.checkAndPut(row, family, qualifier, value, put);
	}
	
	/***
	 * simple wrap of checkAndPut method from HTable. No matter the column
	 * exists or not, this method set the specific value.
	 * 
	 * @param table
	 * @param row
	 * @param family
	 * @param column
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public boolean forceCheckAndPut(byte[] table, byte[] row, byte[] family,
			byte[] column, byte[] value) throws IOException {
		HTable htable = (HTable) TestBase.getTablePool().getTable(table);
		Put put = new Put(row);
		put.add(family, column, value);
		boolean rt = false;

		try {
			Get get = new Get(row);
			get.addColumn(family, column);
			Result r = htable.get(get);
			if (r.isEmpty()) {
				rt = htable.checkAndPut(row, family, column, null, put);
			} else {
				rt = htable.checkAndPut(row, family, column,
						r.getValue(family, column), put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			htable.close();
		}
		return rt;
	}

	/***
	 * simple wrap of checkAndPut method from HTable. No matter the column
	 * exists or not, this method set the specific value.
	 * 
	 * @param table
	 * @param row
	 * @param family
	 * @param column
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public boolean forceCheckAndPut(String table, String row, String family,
			String column, String value) throws IOException {
		return forceCheckAndPut(Bytes.toBytes(table), Bytes.toBytes(row),
				Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

}
