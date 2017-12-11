
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
     * The set of requested Prefetches that have not been consumed by a `read`.
     */
    private HashMap<String, TreeSet<Prefetch>> prefetches;

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
        this.memAllowed = 1 << 30; // 1GB... TODO: how do we choose this?
        String memory_limit = System.getenv("PREFETCH_MEM_LIMIT");
        if (memory_limit != null) {
            int mem_limit = Integer.parseInt(memory_limit);
            this.memAllowed = mem_limit * (1 << 20);
        }
        this.prefetches = new HashMap<>();
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

            LOG.info("Prefetch.read: synchronized 1 being executed");
            synchronized (this.policy) {
                predicted = this.policy.next(filename,
                                             offset,
                                             buf.length,
                                             this.buffer.memUsage(),
                                             memAllowed,
                                             reducerId);
            }
            LOG.info("Prefetch.read: synchronized 1 finished");

            if (predicted != null) {
                this.buffer.prefetch(predicted.filename,
                                     predicted.offset,
                                     predicted.length);
                LOG.info("Prefetch.read: marking request" + filename);
                markRequested(predicted);
            }

            LOG.info("Prefetch.read: synchronized 2 being executed");
            synchronized (this) {
                if (wasRequested(filename, offset, buf.length)) {
                    LOG.info("Prefetch.read: unmarking request" + filename);
                    unmarkRequested(filename, offset, buf.length);

                    // Issue a read to the buffer to get the data for this request.
                    return this.buffer.read(filename, offset, buf);
                }
            }
            LOG.info("Prefetch.read: synchronized 2 finished");

            try {
                Thread.sleep(100); // 100ms
            } catch (InterruptedException ie) {
                // It is ok!
            }
        }
    }

    /**
     * @return if some thread has previously called `prefetch` for the given
     * range of the given file.
     */
    private boolean wasRequested(String filename, long offset, long length) {
        long soFar = offset;

        while (soFar < offset + length) {
            long amt = wasOffsetRequested(filename, soFar);

            if (amt < 0) {
                return false;
            }

            soFar += amt;
        }

        return true;
    }

    /**
     * @return -1 if the given offset of the given file was never requested, or
     * some positive number of bytes requested otherwise.
     */
    private long wasOffsetRequested(String filename, long offset) {
        // Get prefetches for this file
        TreeSet<Prefetch> fPref = this.prefetches.get(filename);

        // No prefetches?
        if (fPref == null) {
            return -1;
        }

        // Look for the greatest prefetch that starts before at the given offset
        Prefetch pref = fPref.floor(
                new Prefetch(filename,
                             offset,
                             0));

        // No prefetches?
        if (pref == null) {
            return -1;
        }

        // Check if that prefetch covers any of the given range
        if (pref.offset + pref.length <= offset) {
            return -1;
        }

        return pref.offset + pref.length - offset;
    }

    /**
     * Mark the given prefetch as requested.
     */
    private void markRequested(Prefetch predicted) {
        // If this is the first request for the given file, then things are easy.
        TreeSet<Prefetch> fPref = this.prefetches.get(predicted.filename);
        if (fPref == null) {
            fPref = new TreeSet<>(new PrefetchComparator());
            fPref.add(predicted);
            this.prefetches.put(predicted.filename, fPref);
            return;
        }

        // Look for the greatest prefetch that starts before at the given offset
        Prefetch pref = fPref.floor(predicted);

        // No prefetches? Things are easy...
        if (pref == null) {
            fPref.add(predicted);
            return;
        }

        // Otherwise, we want to add the new prefetch. Avoid adding a prefetch
        // that is completely subsumed by an existing one.
        if (pref.offset + pref.length >= predicted.offset + predicted.length) {
            return;
        }

        // If the new prediction subsumes others, remove the others.
        Prefetch dummyEnd = new Prefetch(predicted.filename,
                                         predicted.offset + predicted.length, 0);
        SortedSet<Prefetch> subsumed = fPref.subSet(predicted, dummyEnd);

        for (Prefetch p : subsumed) {
            if (p.offset + p.length <= predicted.offset + predicted.length) {
                fPref.remove(p);
            }
        }

        // Then add the new one
        fPref.add(predicted);
    }

    /**
     * Unmark the given prefetch as requested, so that future reads must
     * request it again.
     */
    private void unmarkRequested(String filename, long offset, long length) {
        // Get the set of prefetches for this file
        TreeSet<Prefetch> fPref = this.prefetches.get(filename);
        if (fPref == null) {
            fPref = new TreeSet<>(new PrefetchComparator());
            this.prefetches.put(filename, fPref);
        }

        // Get the overlapping range of issued prefetches
        Prefetch dummyStart = new Prefetch(filename, offset, 0);
        Prefetch dummyEnd = new Prefetch(filename, offset + length, 0);
        SortedSet<Prefetch> covered_ = fPref.subSet(dummyStart, dummyEnd);
        TreeSet<Prefetch> covered = new TreeSet<>(new PrefetchComparator());
        covered.addAll(covered_);

        // Add in the first node if needed
        Prefetch first = fPref.floor(dummyStart);
        if (first != null) {
            covered.add(first);
        }

        // Remove or hole-punch each Prefetch found
        for (Prefetch p : covered) {
            // Is p completely contained in the range?
            if (p.offset >= offset && p.offset + p.length <= offset + length) {
                fPref.remove(p); // Completely remove
            } else {
                // Remove from the set so we can add others
                fPref.remove(p);

                // If p starts before offset or ends after (offset + length) or
                // both, then we should remove only the overlapping part.
                if (p.offset < offset && p.offset + p.length > offset) {
                    Prefetch front = new Prefetch(filename,
                                                  p.offset,
                                                  offset - p.offset);
                    fPref.add(front);
                }

                if (p.offset + p.length > offset + length) {
                    Prefetch back = new Prefetch(filename,
                                                 offset + length,
                                                 (p.offset + p.length)
                                                 - (offset + length));
                    fPref.add(back);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Comparator for Prefetches
    ///////////////////////////////////////////////////////////////////////////
    private class PrefetchComparator implements Comparator<Prefetch> {
        public int compare(Prefetch p1, Prefetch p2) {
            return (int)(p1.offset - p2.offset);
        }
    }
}
