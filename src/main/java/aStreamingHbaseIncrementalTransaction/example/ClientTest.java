package aStreamingHbaseIncrementalTransaction.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ClientTest {
	private static final String TABLE_NAME = "SimpleDungbeeTest";
	private static final String TABLE_NAME_2 = "SimpleDungbeeTest2";
	private static final String COLUMN_FAMILY = "SimpleColumnFamily";
	private static Configuration conf;
	private static HTablePool pool;
	
	static	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort" , "42181");
		conf.set("hbase.zookeeper.quorum", "node01");
		conf.set("hbase.master", "node01:62000");
	}
	
	public static void createTable(String tableName , String columnFamily)	{
		System.out.println("#ClientTest.createTable: Create Table " + tableName);
		try	{
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if(hBaseAdmin.tableExists(tableName))	{
				System.out.println("#ClientTest.createTable: table " + tableName + " exist.");
				return;
			}
			HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
			hTableDesc.addFamily(new HColumnDescriptor(columnFamily));
			hBaseAdmin.createTable(hTableDesc);
		}	catch(Exception e)	{
			e.printStackTrace();
		}
		System.out.println("#ClientTest.creteTable: End Create.");
	}
	
	public static void insertRow(String tableName , byte[] columnFamily ,
		byte[] qualifier) throws IOException	{
		
		HTable hTable = (HTable) pool.getTable(tableName);
		List<Put> putList = new ArrayList<Put>();
		Put put;
		
		for(int i = 1 ; i != 10 ; ++i)	{
			put = new Put(("row"+i).getBytes());
			put.add(columnFamily, qualifier, ("value"+i).getBytes());
			putList.add(put);
		}
		hTable.put(putList);
		hTable.close();
	}
	
	public static void queryTable(String tableName)	{
		
		HTable hTable = (HTable) pool.getTable(tableName);
		ResultScanner rs = null;
		try	{
			rs = hTable.getScanner(new Scan());
			for(Result r : rs)	{
				System.out.println("#ClientTest.queryTable: rowKey = " + new String(r.getRow()));
				for(KeyValue kv : r.raw())	{
					System.out.println("	$row			: " + new String(kv.getRow()));
					System.out.println("	$columnFamily	: " + new String(kv.getFamily()));
					System.out.println("	$columnQualifier: " + new String(kv.getQualifier()));
					System.out.println("	$value			: " + new String(kv.getValue()));
				}
			}
		}	catch (IOException e)	{
			e.printStackTrace();
		}	finally	{
			rs.close();
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException	{
		pool = new HTablePool(conf , 100);
		createTable(TABLE_NAME , COLUMN_FAMILY);
		createTable(TABLE_NAME_2 , COLUMN_FAMILY);
		insertRow(TABLE_NAME, COLUMN_FAMILY.getBytes(), "column1".getBytes());
		insertRow(TABLE_NAME_2, COLUMN_FAMILY.getBytes(), "column1".getBytes());
		queryTable(TABLE_NAME);
		queryTable(TABLE_NAME_2);
		pool.close();
	}
}






