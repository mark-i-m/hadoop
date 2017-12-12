
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.NopPolicy;
import org.apache.hadoop.mapred.RandomAccessPolicy;
import org.apache.hadoop.mapred.BalancedWorkloadPolicy;
import org.apache.hadoop.mapred.UnbalancedWorkloadPolicy;
import org.apache.hadoop.mapred.PrefetchBuffer;
import org.apache.hadoop.mapred.PrefetchPolicy;
import org.apache.hadoop.mapred.PrefetchPolicy.Prefetch;

public class Prefetcher {

    ////////////////////////////////////////////////////////////////////////////
    // Fields
    ////////////////////////////////////////////////////////////////////////////

    /**
     * The single global instance of this object that everyone should use.
     */
    public static final Prefetcher PREFETCHER = new Prefetcher();

    /**
     * The prefetch policy to be used by the prefetcher.
     */
    private final PrefetchPolicy policy;

    /**
     * The prefetch buffer this prefetcher uses.
     *
     * All prefetches/reads the prefetcher does are issued to disk through this
     * object.
     */
    private final PrefetchBuffer buffer;

    /**
     * The total amoung (in bytes) of memory this prefetcher is allowed to use.
     */
    private long memAllowed;

    /**
     * Log object for logging...
     */
    private final Log LOG = LogFactory.getLog(Prefetcher.class);

    ////////////////////////////////////////////////////////////////////////////
    // Methods
    ////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new Prefetcher...
     */
    private Prefetcher() {
        this.buffer = new PrefetchBuffer();
        String policy_selector = System.getenv("PREFETCH_POLICY");
        if ("random".equalsIgnoreCase(policy_selector)) {
            LOG.info("Choosing RandomAccessPolicy");
            this.policy = new RandomAccessPolicy();
        }
        else if ("balanced".equalsIgnoreCase(policy_selector)) {
            LOG.info("Choosing BalancedWorkloadPolicy");
            this.policy = new BalancedWorkloadPolicy();
        }
        else if ("unbalanced".equalsIgnoreCase(policy_selector)) {
            LOG.info("Choosing UnbalancedWorkloadPolicy");
            this.policy = new UnbalancedWorkloadPolicy();
        }
        else {
            LOG.info("Choosing NopPolicy");
            this.policy = new NopPolicy();
        }
        this.memAllowed = 1l << 30; // 1GB... TODO: how do we choose this?
        String memory_limit = System.getenv("PREFETCH_MEM_LIMIT");
        if (memory_limit != null) {
            long mem_limit = Long.parseLong(memory_limit);
            this.memAllowed = mem_limit << 20;
        }
        assert(this.memAllowed > 0);
    }

    /**
     * Read the given section of the given file from this buffer into the given
     * buffer, filling the given buffer completely if there are enough bytes to
     * do so.
     *
     * @param filename the file to read from
     * @param offset the offset into the file to read
     * @param buf the buffer into which to read
     * @param reducerId the ID of the reducer reading (used for prefetching)
     * @return the number of bytes read
     */
    public int read(String filename, long offset, byte[] buf, int reducerId)
        throws IOException
    {
        LOG.info("Prefetcher.read " + filename
                + " off=" + offset + " len=" + buf.length);

        while (true) {
            Prefetch predicted;

            //LOG.info("Prefetch.read: synchronized 1 being executed");
            synchronized (this.policy) {
                predicted = this.policy.next(filename,
                                             offset,
                                             buf.length,
                                             this.buffer.memUsage(),
                                             this.memAllowed,
                                             reducerId);
            }
            //LOG.info("Prefetch.read: synchronized 1 finished");

            if (predicted != null) {
                this.buffer.prefetch(predicted.filename,
                                     predicted.offset,
                                     predicted.length);
            }

            //LOG.info("Prefetch.read: synchronized 2 being executed");
            synchronized (this) {
                if (this.buffer.wasRequested(filename, offset, buf.length)) {
                    // Issue a read to the buffer to get the data for this request.
                    return this.buffer.read(filename, offset, buf);
                }
            }
            //LOG.info("Prefetch.read: synchronized 2 finished");

            try {
                Thread.sleep(100); // 100ms
            } catch (InterruptedException ie) {
                // It is ok!
            }
        }
    }
}
