package aStreamingHbaseIncrementalTransaction.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ClientTest {
	public static final String TABLE_NAME = "SimpleDungbeeTest";
	public static final String COLUMN_FAMILY = "SimpleColumnFamily";
	public static Configuration conf;
	
	static	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort" , "42181");
		conf.set("hbase.zookeeper.quorun", "192.168.226.211");
		conf.set("hbase.master", "192.168.226.211:62000");
	}
	
	public static void createTable(String tableName , String ColumnFamily)	{
		System.out.println("#ClientTest.createTable: Create Table " + tableName);
		try	{
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if(hBaseAdmin.tableExists(tableName))	{
				System.out.println("#ClientTest.createTable: table exist.");
				return;
			}
			HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
			hTableDesc.addFamily(new HColumnDescriptor(ColumnFamily));
			hBaseAdmin.createTable(hTableDesc);
		}	catch(Exception e)	{
			e.printStackTrace();
		}
		System.out.println("#ClientTest.creteTable: End Create.");
	}
	
	public static void main(String[] args)	{
		createTable(TABLE_NAME , COLUMN_FAMILY);
	}
}
