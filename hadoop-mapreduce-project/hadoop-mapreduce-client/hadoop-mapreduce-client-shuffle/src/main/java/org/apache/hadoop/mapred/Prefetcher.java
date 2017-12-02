
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.mapred.NopPolicy;
import org.apache.hadoop.mapred.PrefetchBuffer;
import org.apache.hadoop.mapred.PrefetchPolicy;

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
            PrefetchPolicy.Prefetch predicted;

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
        return -1; // TODO
    }

    /**
     * Mark the given prefetch as requested.
     */
    private void markRequested(PrefetchPolicy.Prefetch predicted) {
        // TODO
    }

    /**
     * Unmark the given prefetch as requested, so that future reads must
     * request it again.
     */
    private void unmarkRequested(String filename, long offset, long length) {
        // TODO
    }
}
