
package org.apache.hadoop.mapred;

import java.util.LinkedList;

import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;

/**
 * A policy that is optimized for lots of reducers making small reads.
 */
public class RandomAccessPolicy implements PrefetchPolicy {
    /**
     * A queue of Prefetches to be made
     */
    private LinkedList<Prefetch> queue = new LinkedList<>();

    /**
     * If we have enough memory, then just use the oldest waiting prefetch.
     * Otherwise, issue no prefetch for now.
     */
    public Prefetch next(String filename,
                         long offset,
                         long length,
                         long memUsage,
                         long memBound,
                         int reducerId)
    {
        // TODO: maybe we want to break up large requests?

        // Enqueue the new request
        Prefetch prefetch = new Prefetch(filename, offset, length);
        queue.addLast(prefetch);

        // If the oldest request does not take up too much space, dequeue it
        // and return it.
        Prefetch oldest = queue.peekFirst();
        if (oldest != null && oldest.length < (memBound - memUsage)) {
            return queue.pollLast();
        } else {
            return null;
        }
    }
}
