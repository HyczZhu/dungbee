package aStreamingHbaseIncrementalTransaction.observer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestMain {
	
	private static final String TABLE_NAME = "SimpleDungbeeTest";
	private static final String COLUMN_FAMILY = "SimpleColumnFamily";
	private static final String COLUMN_QUALIFY = "SimpleColumnQualify";
	
	public static void insertRow(String tableName, byte[] columnFamily,
			byte[] qualifier) throws IOException {

		HTable hTable = (HTable) ColumnAggregationClient.getTablePool().getTable(tableName);
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
	
	public static void main(String[] args){
		System.out.println("holy shit");
		
		try {
			insertRow(TABLE_NAME,COLUMN_FAMILY.getBytes(),COLUMN_QUALIFY.getBytes());
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		ColumnAggregationClient endpoint = new ColumnAggregationClient();
		
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(1));
		scan.setStopRow(Bytes.toBytes(9));
		
		try {
			endpoint.sum(TABLE_NAME.getBytes(), scan);
		} catch (Throwable e) {

			e.printStackTrace();
		}
	}
}
