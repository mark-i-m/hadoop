
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.mapred.PrefetchBuffer;

public class Prefetcher {
    /**
     * Represents a single stream that this prefetcher is tracking.
     */
    private class PrefetchStream {
        // The location and length of this stream
        private String filename;
        private long offset;
        private long length;

        // The number of bytes from this stream that have been prefetched so far...
        private long prefetched;

        /**
         * Construct a new prefetch stream for the given location and length.
         * @param filename the file to read from
         * @param offset the offset into the file to read
         * @param length the length of the stream
         */
        public PrefetchStream(String filename, long offset, long length) {
            this.filename = filename;
            this.offset = offset;
            this.length = length;
            this.prefetched = 0;
        }

        /**
         * Issues a prefetch from this stream to the given buffer.
         * @param amount the max number of bytes to prefetch (it may be shorter
         * if the stream ends)
         */
        public void prefetch(PrefetchBuffer buffer, long amount)
            throws FileNotFoundException, IOException
        {
            // How much is remaining in this stream?
            long remaining = this.length - this.prefetched;
            long amtPrefetch = Math.min(amount, remaining);

            // Issue a prefetch request to the buffer
            buffer.prefetch(this.filename,
                            latestPrefetched(),
                            amtPrefetch);

            // Update bookkeeping
            this.prefetched += amtPrefetch;
        }

        /**
         * @return true if the given location is in this stream. Otherwise, false.
         */
        public boolean contains(String filename, long offset) {
            // FIXME: need to normalize file names first to deal with e.g. relative
            // vs absolute paths to the same file...
            if (!filename.equals(this.filename)) {
                return false;
            }

            return offset >= this.offset && offset < this.offset + this.length;
        }

        /**
         * @return the offset _in the file_ of the first byte not yet prefetched.
         */
        public long latestPrefetched() {
            return this.offset + this.prefetched;
        }

        /**
         * Truncate this stream at whatever has already been prefetched.
         */
        public void truncate() {
            this.length = this.prefetched;
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Fields
    ////////////////////////////////////////////////////////////////////////////

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

    /**
     * For each file, keep a list of streams in that file.
     */
    private final HashMap<String, ArrayList<PrefetchStream>> streamInfo;

    /**
     * An LRU list, where the head (first) is the LRU, and the tail (last) is
     * the MRU.
     */
    private final LinkedList<PrefetchStream> lruOrder;

    ////////////////////////////////////////////////////////////////////////////
    // Methods
    ////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new Prefetcher...
     */
    private Prefetcher() {
        this.buffer = new PrefetchBuffer();
        this.streamInfo = new HashMap<>();
        this.lruOrder = new LinkedList<>();
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
        fetchStream(filename, offset, buf.length);

        // Issue a prefetch for the LRU stream. Make sure it is enough to
        // satisfy this request. (FIXME: maybe we want something else here?)
        PrefetchStream stream = getLRUStream();
        stream.prefetch(buffer, buf.length);

        // Update the LRU order.
        moveToMRU(stream);

        // Issue a read to the buffer to get the data for this request.
        return this.buffer.read(filename, offset, buf);
    }

    /** If the request is for something that was not previous prefetched
     * (whether it was never seen or only partially prefetched), make it the
     * LRU stream. Otherwise, do nothing.
     */
    private void fetchStream(String filename, long offset, long length) {
        // Get the list of streams for this file
        ArrayList<PrefetchStream> fileStreams =
            this.streamInfo.get(filename);

        if (fileStreams == null) {
            fileStreams = new ArrayList<>();
            this.streamInfo.put(filename, fileStreams);
        }

        // Look for one with the the given offset
        PrefetchStream match = null;
        for (PrefetchStream ps : fileStreams) {
            if (ps.contains(filename, offset)) {
                match = ps;
                break;
            }
        }

        // If one is found, check what the most recently prefetched offset in
        // that stream is.
        if (match != null && match.latestPrefetched() >= offset + length) {
            // If the desired portion was prefetched, then we are done
            return;
        }

        // If we got here, the there is some portion of the request
        // that was not already prefetched. Create a new stream for that
        // portion and make it the LRU.
        long notYetStart;
        long notYetLength;

        if (match != null) {
            // There was a stream that was prefetching at least part of the
            // requested portion of the file.
            long end = offset + length;
            notYetStart = match.latestPrefetched();
            notYetLength = end - notYetStart;

            // We are about to create a new stream for the rest of the old
            // stream, so end the old stream here. This simplifies the logic
            // of prefetching because we can always focus on prefetching just
            // a single stream.
            match.truncate();
        } else {
            // No matching existing stream was found, so create one.
            notYetStart = offset;
            notYetLength = length;
        }

        // Create a new stream
        PrefetchStream newStream = new PrefetchStream(filename,
                                                      notYetStart,
                                                      notYetLength);

        // Make it the LRU
        this.streamInfo.get(filename).add(newStream);
        this.lruOrder.addFirst(newStream);
    }

    /**
     * @return the LRU stream (i.e. the one least-recently prefetched).
     *
     * NOTE: the prefetcher's table is assumed to be non-empty!
     */
    private PrefetchStream getLRUStream() {
        return this.lruOrder.getFirst();
    }

    /**
     * Move the given stream to the MRU position.
     *
     * NOTE: the given stream is assumed to be in the prefetcher's table
     * already!
     */
    private void moveToMRU(PrefetchStream stream) {
        // Remove from the LRU list
        boolean exists = this.lruOrder.remove(stream);
        assert(exists);

        // Append to the tail (MRU)
        this.lruOrder.addLast(stream);
    }
}
