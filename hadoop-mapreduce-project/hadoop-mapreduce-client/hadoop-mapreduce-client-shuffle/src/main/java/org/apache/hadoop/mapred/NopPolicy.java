
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.PrefetchPolicy;

/**
 * A policy that makes no predictions but just forwards all requests to the
 * prefetcher.
 */
public class NopPolicy implements PrefetchPolicy {
    public PrefetchPolicy.Prefetch next(String filename,
                                        long offset,
                                        long length,
                                        long memUsage,
                                        long memBound)
    {
        return new PrefetchPolicy.Prefetch(filename, offset, length);
    }
}
