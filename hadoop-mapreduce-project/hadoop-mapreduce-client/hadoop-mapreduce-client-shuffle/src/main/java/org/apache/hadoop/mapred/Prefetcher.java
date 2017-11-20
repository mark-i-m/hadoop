
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapred.PrefetchBuffer;

public class Prefetcher {
    /**
     * Represents a single stream that this prefetcher is tracking.
     */
    private class PrefetchStream {
        /**
         * Issues a prefetch from this stream to the given buffer.
         */
        public void prefetch(PrefetchBuffer buffer) {
            // TODO
        }
    }

    /**
     * The single global instance of this object that everyone should use.
     */
    public static final Prefetcher PREFETCHER = new Prefetcher();

    /**
     * The prefetch buffer this prefetcher uses.
     *
     * All prefetches/reads the prefetcher does are issued to disk through this
     * object.
     */
    private final PrefetchBuffer buffer;

    private Prefetcher() {
        buffer = new PrefetchBuffer();
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
    public synchronized int read(
        // FIXME synchronized here means we always lock the whole prefetcher...
        // maybe we want to do something finer-grain.
            String filename,
            long offset,
            byte[] buf)
        throws IOException
    {
        // If the request is for something that was not previous prefetched
        // (whether it was never seen or only partially prefetched), make it
        // the LRU stream.
        fetchStream(filename, offset);

        // Issue a prefetch for the LRU stream. Make sure it is enough to
        // satisfy this request. (FIXME: maybe we want something else here?)
        PrefetchStream stream = getLRUStream();
        stream.prefetch(buffer);

        // Update the LRU order.
        moveToMRU(stream);

        // Issue a read to the buffer to get the data for this request.
        return buffer.read(filename, offset, buf);
    }

    /** If the request is for something that was not previous prefetched
     * (whether it was never seen or only partially prefetched), make it the
     * LRU stream. Otherwise, do nothing.
     */
    private void fetchStream(String filename, long offset) {
        // TODO
    }

    /**
     * @return the LRU stream (i.e. the one least-recently prefetched).
     */
    private PrefetchStream getLRUStream() {
        // TODO
        return new PrefetchStream();
    }

    /**
     * Move the given stream to the MRU position.
     */
    private void moveToMRU(PrefetchStream stream) {
        // TODO
    }
}
