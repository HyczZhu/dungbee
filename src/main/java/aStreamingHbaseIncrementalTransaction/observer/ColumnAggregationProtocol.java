package aStreamingHbaseIncrementalTransaction.observer;

import java.io.IOException;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface ColumnAggregationProtocol
extends CoprocessorProtocol {
  // Perform aggregation for a given column at the region. The aggregation
  // will include all the rows inside the region. It can be extended to
  // allow passing start and end rows for a fine-grained aggregation.
  public long sum(byte[] family, byte[] qualifier) throws IOException;
}
