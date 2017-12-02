
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

import org.apache.hadoop.mapred.NopPolicy;
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

    ////////////////////////////////////////////////////////////////////////////
    // Methods
    ////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new Prefetcher...
     */
    private Prefetcher() {
        this.buffer = new PrefetchBuffer();
        this.policy = new NopPolicy();
        this.memAllowed = 1 << 30; // 1GB... TODO: how do we choose this?
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
     * @return the number of bytes read
     */
    public int read(String filename, long offset, byte[] buf)
        throws IOException
    {
        System.out.println("Prefetcher.read " + filename
                + " off=" + offset + " len=" + buf.length);

        while (true) {
            Prefetch predicted;

            synchronized (this.policy) {
                predicted = this.policy.next(filename,
                                             offset,
                                             buf.length,
                                             this.buffer.memUsage(),
                                             memAllowed);
            }

            if (predicted != null) {
                this.buffer.prefetch(predicted.filename,
                                     predicted.offset,
                                     predicted.length);
                markRequested(predicted);
            }

            synchronized (this) {
                if (wasRequested(filename, offset, buf.length)) {
                    unmarkRequested(filename, offset, buf.length);

                    // Issue a read to the buffer to get the data for this request.
                    return this.buffer.read(filename, offset, buf);
                }
            }

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

        // If the new prediction is longer than the old one and has the same
        // start offset, replace the old one.
        if (pref.offset == predicted.offset && pref.length < predicted.length) {
            fPref.remove(pref);
            fPref.add(predicted);
        }
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
        SortedSet<Prefetch> covered = fPref.subSet(dummyStart, dummyEnd);

        // Add in the first node if needed
        Prefetch first = fPref.floor(dummyStart);
        if (first != null) {
            covered.add(first);
        }

        // Remove or hole-punch each Prefetch found
        for (Prefetch p : covered) {
            // Is p completely contained in the range?
            if (p.offset >= offset && p.offset + p.length <= offset + length) {
                covered.remove(p); // Completely remove
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
                                                 (p.offset + p.length) - (offset + length));
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
