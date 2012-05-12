package aStreamingHbaseIncrementalTransaction.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseBasicClient {
	private static final String TABLE_NAME = "SimpleDungbeeTest";
	private static final String TABLE_NAME_2 = "SimpleDungbeeTest2";
	private static final String COLUMN_FAMILY = "SimpleColumnFamily";
	private static Configuration conf;
	private static HTablePool pool;
	
	static {
		pool = TestBase.getTablePool();
	}

	public static void createTable(String tableName, String columnFamily) {
		System.out
				.println("#ClientTest.createTable: Create Table " + tableName);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {
				System.out.println("#ClientTest.createTable: table "
						+ tableName + " exist.");
				return;
			}
			HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
			hTableDesc.addFamily(new HColumnDescriptor(columnFamily));
			hBaseAdmin.createTable(hTableDesc);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("#ClientTest.creteTable: End Create.");
	}

	public static void insertRow(String tableName, byte[] columnFamily,
			byte[] qualifier) throws IOException {

		HTable hTable = (HTable) pool.getTable(tableName);
		List<Put> putList = new ArrayList<Put>();
		Put put;

		for (int i = 1; i != 10; ++i) {
			put = new Put(("row" + i).getBytes());
			put.add(columnFamily, qualifier, ("value" + i).getBytes());
			putList.add(put);
		}
		hTable.put(putList);
		hTable.close();
	}

	static void printResultScanner(ResultScanner rs) {
		if (rs==null) return;
		for (Result r : rs) {
			System.out.println("####################################################");
			System.out.println("#printResultScanner: rowKey = "
					+ new String(r.getRow()));
			for (KeyValue kv : r.raw()) {
				System.out.println("	$row			: " + new String(kv.getRow()));
				System.out.println("	$columnFamily	: "
						+ new String(kv.getFamily()));
				System.out.println("	$columnQualifier: "
						+ new String(kv.getQualifier()));
				System.out.println("	$value			: " + new String(kv.getValue()));
				System.out.println("	$timpstamp		: " + kv.getTimestamp());
				System.out.println("----------------------------------------------------");
			}
		}
	}

	static void printResult(Result r) {
		if (r == null || r.isEmpty()) return;
		System.out.println("####################################################");
		System.out.println("#printResult: rowKey = "
				+ new String(r.getRow()));
		System.out.println("printResult: size = "+r.size());
		for (KeyValue kv : r.raw()) {
			System.out
					.println("	$columnFamily	: " + new String(kv.getFamily()));
			System.out.println("	$columnQualifier: "
					+ new String(kv.getQualifier()));
			System.out.println("	$value			: " + new String(kv.getValue()));
			System.out.println("	$timpstamp		: " + kv.getTimestamp());
			System.out.println("----------------------------------------------------");
		}
	}

	public static void queryTable(String tableName) {

		HTable hTable = (HTable) pool.getTable(tableName);
		ResultScanner rs = null;
		// Get get=new Get("row1".getBytes());

		try {
			rs = hTable.getScanner(new Scan());
			printResultScanner(rs);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			rs.close();
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void queryRow(String tableName, String rowName){
		HTable hTable = (HTable) pool.getTable(tableName);
		// Get get=new Get("row1".getBytes());
		Result r = null;

		try {
			Get get = new Get(Bytes.toBytes(rowName));
			//get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			r = hTable.get(get);
			printResult(r);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void queryFamily(String tableName, String rowName,
			String familyName) {

		HTable hTable = (HTable) pool.getTable(tableName);
		// Get get=new Get("row1".getBytes());
		Result r = null;

		try {
			Get get = new Get(Bytes.toBytes(rowName));
			get.addFamily(Bytes.toBytes(familyName));
			//get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			r = hTable.get(get);
			printResult(r);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Begin");
		
		// createTable(TABLE_NAME , COLUMN_FAMILY);
		// createTable(TABLE_NAME_2 , COLUMN_FAMILY);
		// insertRow(TABLE_NAME, COLUMN_FAMILY.getBytes(),
		// "column1".getBytes());
		// insertRow(TABLE_NAME_2, COLUMN_FAMILY.getBytes(),
		// "column1".getBytes());
		queryTable(TABLE_NAME);
		queryTable(TABLE_NAME_2);
		pool.close();
		System.out.println("End");

	}
}
