package aStreamingHbaseIncrementalTransaction.observer;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

// Sample access-control coprocessor. It utilizes RegionObserver
// and intercept preXXX() method to check user privilege for the given table
// and column family.
public class AccessControlCoprocessor extends BaseRegionObserver {
  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c,
final Get get, final List<KeyValue> result) throws IOException{

    // check permissions..
	  permissionGranted();
  }

  // override prePut(), preDelete(), etc.
  
  private boolean permissionGranted()
  {
	  System.out.println("bigin permission Granted function!");
	  return true;
	  
  }
}
