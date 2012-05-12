package aStreamingHbaseIncrementalTransaction.multirowTransaction;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import aStreamingHbaseIncrementalTransaction.example.TestBase;
import aStreamingHbaseIncrementalTransaction.example.TestHBaseBasicClient;

public class TestRowTransaction extends RowTransaction {
	private static final String TABLE_NAME = "SimpleDungbeeTest";
	private static final String TABLE_NAME_2 = "SimpleDungbeeTest2";
	private static final String COLUMN_FAMILY = "SimpleColumnFamily";

	public static int threadcount = 0;

	/***
	 * use multi thread to lock and put data into a row. using some default put
	 * parameters
	 * 
	 * @param threadNum
	 */
	public void multiThreadLockAndPut(int threadNum) {
		multiThreadLockAndPut(threadNum, TABLE_NAME,
				"row_multiThreadLockAndPut", COLUMN_FAMILY, "column",
				"multiThreadLockAndPut");
	}

	/***
	 * use multi thread to lock and put data into a row
	 * 
	 * @param threadNum
	 * @param tableName
	 * @param rowName
	 * @param familyName
	 * @param columnName
	 * @param value
	 */
	public void multiThreadLockAndPut(int threadNum, final String tableName,
			final String rowName, final String familyName,
			final String columnName, final String value) {
		for (int i = 0; i < threadNum; i++) {
			final int tnum = i;
			Thread t = new Thread(new Runnable() {
				public void run() {
					TestRowTransaction instance = new TestRowTransaction();
					try {
						instance.lockAndPut(tableName, rowName, familyName,
								columnName + tnum, value + tnum);
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						TestRowTransaction.threadcount++;
					}
				}
			});
			t.start();
		}
	}



	public static void main(String[] args) {
		System.out.println("Begin");
		try {
			TestRowTransaction trt = new TestRowTransaction();
			// for (int i = 0; i < 4; i++) {
			// new TestRowTransaction().lockAndPut(TABLE_NAME, "row" + i,
			// COLUMN_FAMILY, "column",
			// "lockAndPut-value" + i);
			// TestHBaseBasicClient.queryFamily(TABLE_NAME, "row" + i,
			// COLUMN_FAMILY);
			// }
			// TestHBaseBasicClient.queryTable(TABLE_NAME);
			// ///////////////
			// int threadnum = 4;
			// trt.multiThreadLockAndPut(threadnum);
			// while (true) {
			// if (TestRowTransaction.threadcount == threadnum) {
			// TestHBaseBasicClient.queryRow(TABLE_NAME,
			// "row_multiThreadLockAndPut");
			// break;
			// }
			// }
			// ////////////////
			for (int i = 0; i < 6; i++) {
				boolean b;
				b = trt.forceCheckAndPut(TABLE_NAME, "row_checkAndPut",
						COLUMN_FAMILY, "column" + i, "checkAndPut-value" + i);
				System.out.println("i=" + i + ";result=" + b);
			}
			TestHBaseBasicClient.queryRow(TABLE_NAME, "row_checkAndPut");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
