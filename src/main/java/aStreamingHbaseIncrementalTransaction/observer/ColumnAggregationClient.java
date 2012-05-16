package aStreamingHbaseIncrementalTransaction.observer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

public class ColumnAggregationClient {
	private static final String COLUMN_FAMILY = "SimpleColumnFamily";
	private static final String COLUMN_QUALIFY = "SimpleColumnQualify";
	
	private static Configuration conf;
	private static HTablePool pool;
	static	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort" , "42181");
		//conf.set("hbase.zookeeper.quorum", "211.69.198.68");
		//conf.set("hbase.master", "211.69.198.68:62000");
		conf.set("hbase.zookeeper.quorum", "192.168.226.211");
		conf.set("hbase.master", "192.168.226.211:62000");
		pool = new HTablePool(conf , 100);
	}
	public static Configuration getConf(){
		return conf;
	}
	
	public static HTablePool getTablePool(){
		return pool;
	}

	 public  Long sum(final byte[] tableName, final Scan scan) throws Throwable {
		    //validateParameters(scan);
		    HTable table = new HTable(conf, tableName);

		    class MaxCallBack implements Batch.Callback<Long> 
		    {
		      Long max = null;

		      Long getMax() {
		        return max;
		      }

			@Override
			public synchronized void update(byte[] region, byte[] row, Long result) {
				// TODO Auto-generated method stub
				max = new Long(11111111);
			}
		    }
		    MaxCallBack aMaxCallBack = new MaxCallBack();
		    table.coprocessorExec(ColumnAggregationEndpoint.class, scan.getStartRow(), scan
		        .getStopRow(), new Batch.Call<ColumnAggregationEndpoint, Long>() {
		      @Override
		      public Long call(ColumnAggregationEndpoint instance) throws IOException {
		        return instance.sum(COLUMN_FAMILY.getBytes(), COLUMN_QUALIFY.getBytes());
		      }
		    }, aMaxCallBack);
		    return aMaxCallBack.getMax();
	}
	
}
