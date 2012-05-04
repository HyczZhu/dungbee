package aStreamingHbaseIncrementalTransaction.timestampOracle;

import java.net.InetAddress;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;


public class TimestampGen {

	// A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long clock = new Random(System.currentTimeMillis()).nextLong();
    
    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final TimestampGen instance = new TimestampGen();
    
    private long lastNanos;
    private final Map<InetAddress, Long> nodeCache = new HashMap<InetAddress, Long>();
    
    private TimestampGen()
    {
        // make sure someone didn't whack the clock by changing the order of instantiation.
        if (clock == 0) throw new RuntimeException("singleton instantiation is misplaced.");
    }
    
    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    private synchronized long createTimeSafe()
    {
        long nanosSince = (System.currentTimeMillis() - START_EPOCH) * 10000;
        if (nanosSince > lastNanos)
            lastNanos = nanosSince;
        else
            nanosSince = ++lastNanos;
        
        return createTime(nanosSince);
    }
    
    private long createTime(long nanosSince)
    {   
        long msb = 0L; 
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16; 
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }
    
}
