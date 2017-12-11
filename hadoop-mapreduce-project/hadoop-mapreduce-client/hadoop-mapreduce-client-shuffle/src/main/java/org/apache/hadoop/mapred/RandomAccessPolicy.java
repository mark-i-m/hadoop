
package org.apache.hadoop.mapred;

import java.util.LinkedList;

import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A policy that is optimized for lots of reducers making small reads.
 */
public class RandomAccessPolicy implements PrefetchPolicy {
    /**
     * A queue of Prefetches to be made
     */
    private LinkedList<Prefetch> queue = new LinkedList<>();

    /**
     * Log object for logging...
     */
    private static final Log LOG = LogFactory.getLog(RandomAccessPolicy.class);

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
        LOG.info("Request: file=" + filename + ", off=" + offset + ", len=" + length
                  + ", memUsage=" + memUsage + ", memBound=" + memBound
                  + ", reducerId=" + reducerId);

        // Enqueue the new request
        Prefetch prefetch = new Prefetch(filename, offset, length);
        queue.addLast(prefetch);

        // If the oldest request does not take up too much space, dequeue it
        // and return it.
        Prefetch oldest = queue.peekFirst();

        if (oldest == null) {
            LOG.info("Nothing to prefetch");
            return null;
        }

        if (oldest.length > (memBound - memUsage)) {
            LOG.info("Oldest is too large: " + oldest);
            return null;
        }

        LOG.info("Policy chose: " + oldest);
        return queue.pollFirst();
    }
}
