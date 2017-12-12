
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;
import java.io.File;
import java.util.HashMap;

/**
 * A policy based on the assumption that the workload is balanced.
 */
public class BalancedWorkloadPolicy implements PrefetchPolicy {

    // value is true if key has been prefetched
    protected HashMap<Integer, Boolean> prefetched; 
    // TODO: how to obtain?
    int numReduceTasks = 5;     // for now...

    public Prefetch next(String filename,
                         long offset,
                         long length,
                         long memUsage,
                         long memBound,
                         int reducerId)
    {
        long fileSize = (new File(filename)).length();
        String strNumReduceTasks = System.getenv("NUM_REDUCE");
        if (strNumReduceTasks != null)
            numReduceTasks = Integer.parseInt(strNumReduceTasks);
        if (!prefetched.containsKey(reducerId)) {
        	prefetched.put(reducerId, false);
        }
        
        if (prefetched.get(reducerId)) {
        	// assume it has been prefetched
        	return null;
        } else {
        	// issue a new prefetch request
        	long prefetchLength = fileSize / numReduceTasks;
            long startOffset = reducerId * prefetchLength;
        	if (prefetchLength >= memBound - memUsage) {
                startOffset = offset;
        		prefetchLength = length;
        	}
            // TODO: maybe a better idea?
            if (prefetchLength >= memBound - memUsage) return null;
            
            prefetched.put(reducerId, true);
        	return new Prefetch(filename, startOffset, prefetchLength);
        }
    }
}
