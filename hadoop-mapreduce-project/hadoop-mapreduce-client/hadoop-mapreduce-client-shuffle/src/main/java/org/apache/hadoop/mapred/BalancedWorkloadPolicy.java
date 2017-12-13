
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;
import java.io.File;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A policy based on the assumption that the workload is balanced.
 */
public class BalancedWorkloadPolicy implements PrefetchPolicy {

    // value is true if key has been prefetched
    protected HashMap<Integer, Boolean> prefetched = new HashMap<>(); 
    // TODO: how to obtain?
    int numReduceTasks = 1;     // for now...

    private static final Log LOG = LogFactory.getLog(BalancedWorkloadPolicy.class);

    public Prefetch next(String filename,
                         long offset,
                         long length,
                         long memUsage,
                         long memBound,
                         int reducerId)
    {
        if (!prefetched.containsKey(reducerId)) {
        	prefetched.put(reducerId, false);
        }
        
        if (prefetched.get(reducerId)) {
            LOG.info("Nothing to prefetch");
        	// assume it has been prefetched
        	return null;
        } else {
        	// issue a new prefetch request
            long fileSize = (new File(filename)).length();
            String strNumReduceTasks = System.getenv("NUM_REDUCE");
            if (strNumReduceTasks != null)
                numReduceTasks = Integer.parseInt(strNumReduceTasks);
        	long prefetchLength = fileSize / numReduceTasks;
            long startOffset = reducerId * prefetchLength;
        	
            if (prefetchLength >= memBound - memUsage) {
                startOffset = offset;
        		prefetchLength = length;
        	}
            // TODO: maybe a better idea?
            if (prefetchLength >= memBound - memUsage) return null;
            
            prefetched.put(reducerId, true);
            LOG.info("Issue a new prefetch request: " + filename + 
                " offset=" + startOffset + " length=" + prefetchLength);
        	return new Prefetch(filename, startOffset, prefetchLength);
        }
    }
}
