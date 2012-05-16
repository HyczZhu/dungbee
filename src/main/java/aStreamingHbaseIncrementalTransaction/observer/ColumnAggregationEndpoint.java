package aStreamingHbaseIncrementalTransaction.observer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

//Aggregation implementation at a region.
public class ColumnAggregationEndpoint extends BaseEndpointCoprocessor
implements ColumnAggregationProtocol {
  @Override
  public long sum(byte[] family, byte[] qualifier)
  throws IOException {
    // aggregate at each region
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    long sumResult = 0;
    InternalScanner scanner = ( (RegionCoprocessorEnvironment)getEnvironment()).getRegion().getScanner(scan);
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      do {
    curVals.clear();
    hasMore = scanner.next(curVals);
    KeyValue kv = curVals.get(0);
    sumResult += Bytes.toLong(kv.getValue());
      } while (hasMore);
    } finally {
        scanner.close();
    }
    return sumResult;
  }
}
