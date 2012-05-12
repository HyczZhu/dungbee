package aStreamingHbaseIncrementalTransaction.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class TestBase {
	private static Configuration conf;
	private static HTablePool pool;
	static	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort" , "42181");
		conf.set("hbase.zookeeper.quorum", "211.69.198.68");
		conf.set("hbase.master", "211.69.198.68:62000");
		//TODO consider multi thread programming, for now, this is only for a single thread
		pool = new HTablePool(conf , 100);
	}
	
	public static Configuration getConf(){
		return conf;
	}
	
	public static HTablePool getTablePool(){
		return pool;
	}
}
