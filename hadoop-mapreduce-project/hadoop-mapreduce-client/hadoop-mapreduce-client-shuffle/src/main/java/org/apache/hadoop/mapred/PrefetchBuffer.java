
package org.apache.hadoop.mapred;

import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * PrefetchBuffer manages a read-only in-memory buffer for disk reads.
 *
 * Prefetched data is read-once. After it is read, it is discarded and
 * the memory freed for later allocations.
 */
public class PrefetchBuffer {
    /*
     * The prefetch buffer organizes its memory as a big ordered tree
     * of Regions, sorted by their start offset into a given file. i.e.
     * there is a tree of regions for each file being prefetched from.
     *
     * All regions are disjoint; there is no overlap. Thus, finding the
     * regions that contain data for some range of a file is efficient.
     *
     * Reading data from some range of the tree will cause that data to
     * be deallocated and the memory freed.
     *
     * Furthermore, all Regions are inserted into a queue (reqQueue),
     * which the prefetching thread works through in order.
     */
    private HashMap<String, TreeSet<Region>> regions;
    private LinkedList<Region> reqQueue;

    // TODO: start the prefetcher thread!

    ///////////////////////////////////////////////////////////////////////////
    // Public API
    //
    // The public API of the PrefetchBuffer allows a `prefetch` request to be
    // made. This is an asynchronous function call which queues the given region
    // of the given file to be read from disk.
    //
    // Subsequently, a call to `read` will return the data from a given file
    // and offset. A call to `read` must be preceded by a call to `prefetch`.
    // If the data requested has not yet been fetched from disk, then the call
    // to `read` will block until it is fetched. Otherwise, the data will be
    // read directly from the buffer (memory), and that memory freed.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Prefetch the given section of the given file into this buffer.
     *
     * A call to `prefetch` must precede every call to `read`.
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
     * @param length the number of bytes to prefetch
     * @throws FileNotFoundException if the requested file does not exist
     * @throws IOException if there is an error reading the requested file
     *
     * NOTE: this method grabs the monitor lock!
     */
    public synchronized void prefetch(
            String filename,
            long offset,
            long length)
        throws FileNotFoundException, IOException
    {
        // Find unrequested parts of this request
        ArrayList<Region> gaps = findGaps(filename, offset, length);

        // Request all gaps
        insertAndEnqueueAll(filename, gaps);
    }

    /**
     * Read the given section of the given file from this buffer into the given
     * buffer, filling the given buffer completely if there are enough bytes to
     * do so.
     *
     * A call to `prefetch` must precede every call to `read`.
     *
     * This call may block waiting for IO, but hopefully it usually won't :P
     *
     * @param filename the file to prefetch from
     * @param offset the offset into the file to prefetch
     * @param buf the buffer into which to prefetch
     * @return the number of bytes read
     * @throws IOException if there is an error reading the requested file
     */
    public int read(
            String filename,
            long offset,
            byte[] buf)
        throws IOException
    {
        // TODO: for now we just open a file and read from it...
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(filename, "r");
        } catch(FileNotFoundException e) {
            throw new IOException("Unable to open file in dummy impl of read" +
                    filename);
        }
        raf.seek(offset);
        raf.read(buf);
        return buf.length;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Helpers and buffer management
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method is basically a wrapper around TreeSet.subSet that accepts
     * `long` instead of `Region`. It is a convenience method.
     */
    private static SortedSet<Region> getTreeSubSet(TreeSet<Region> tree,
                                                    String filename,
                                                    long start,
                                                    long end)
    {
        // Create a couple of dummy regions so we can call subSet
        Region dummyStart = new Region(filename, start, 0, null, 0);
        Region dummyEnd = new Region(filename, end, 0, null, 0);

        return tree.subSet(dummyStart, dummyEnd);
    }

    /**
     * Finds all previously unrequested Regions in the given range of the file,
     * creates new Regions for them, _allocates_ memory to back IO for them, and
     * returns the new Regions _without inserting or enqueuing them_.
     *
     * NOTE: this call should be made while holding the monitor lock!
     */
    private ArrayList<Region> findGaps(String filename, long offset, long length) {
        // Get the regions for the file
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        // Get a sorted set of all regions covered by the given range of the file
        SortedSet<Region> covered = getTreeSubSet(fRegions,
                                                  filename,
                                                  offset,
                                                  offset + length);

        // Iterate over the `covered` set, keeping track of the most latest
        // offset into the desired range that we have "handled".  Starting with
        // the beginning of the desired range, each sub-range is covered by
        // either a Region from `covered` or a new Region which we will create.
        //
        // Moreover, keep track of the space required for all new Regions, so
        // we can allocate memory afterwards.
        ArrayList<Region> newRegions = new ArrayList<>();
        long requiredSpace = 0;
        long latestHandled = offset;

        for (Region r : covered) {
            long regionStart = r.getStart();

            // If there is space between the beginning of `r` and `offset`, we
            // need to create a new Region. However, since a buffer can only
            // contain so much data, we need to make sure we respect the maximum
            // size of any buffer and break up large Regions appropriately.
            if (regionStart > offset) {
                long spaceNeeded = offset - regionStart;
                long nextOffset = offset;

                while (spaceNeeded > 0) {
                    long newRLen = Math.min(spaceNeeded, MiniBuffer.BUFFER_SIZE);

                    newRegions.add(new Region(filename, nextOffset, newRLen));

                    spaceNeeded -= newRLen;
                    nextOffset += newRLen;
                }

                offset = r.getAfter();
            }
        }

        // allocate memory and update Regions
        MiniBuffer.allocateRegions(newRegions);

        // Done!
        return newRegions;
    }

    /**
     * Sorts, inserts, and enqueues the given list of regions.
     *
     * The sorting is done to minimize seeking.
     *
     * NOTE: this call should be made while holding the monitor lock!
     */
    private void insertAndEnqueueAll(String filename, ArrayList<Region> list) {
        // Sort the regions
        Collections.sort(list);

        if (list.size() == 0) {
            return;
        }

        // Insert into the tree.
        TreeSet<Region> fRegions = this.regions.get(filename);
        if (fRegions == null) {
            fRegions = new TreeSet<>();
            this.regions.put(filename, fRegions);
        }

        fRegions.addAll(list);

        // Enqueue
        this.reqQueue.addAll(list);
    }

    /**
     * Returns a list of requested Regions _exactly_ covering the given the
     * given range of the given file, possibly breaking up the first and last
     * Region to do so.
     *
     * @throws IllegalStateException if the range contains portions of the file
     * which have never been previously requested.
     */
    private synchronized ArrayList<Region> getRange(String filename, long offset, long length) {
        // TODO
        return null;
    }
}

/**
 * A small buffer containing some prefetched data.
 *
 * A single buffer can be shared by multiple Regions disjointly.
 *
 * Consumers of a buffer should call `waitForData` before attempting to read.
 * This will block their thread until IO completes and the data is ready for
 * consumption.
 */
class MiniBuffer {
    public static final int BUFFER_SIZE = 1 << 20; // 1MB

    // The memory where the data is held once IO completes
    private byte[] data = new byte[BUFFER_SIZE];

    // A flag indicating that the data is ready
    private boolean ready = false;

    /**
     * Wait for the data in this buffer to become available.
     */
    public synchronized void waitForData() {
        while (!this.ready) {
            try {
                this.wait();
            } catch (InterruptedException ie) {
                // It is ok!
            }
        }
    }

    /**
     * Wake up all threads waiting for the data.
     */
    public synchronized void dataReady() {
        this.ready = true;
        this.notifyAll();
    }

    /**
     * Allocate buffers for the given regions, and update the Regions
     * to reflect allocations.
     *
     * This method tries to compute the most efficient allocation of
     * memory possible to reduce fragmentation, but it guarantees that
     * on return, all Regions will have an allocation.
     *
     * @throws IllegalArgumentException if any of the Regions is too
     * large (size greater than BUFFER_SIZE).
     */
    public static void allocateRegions(ArrayList<Region> regions) {
        // TODO
    }
}

/**
 * Represents a contiguous region of a file which has yet to be either
 * prefetched or read from disk.
 *
 * Regions are tracked by the buffer in an ordered tree structure, so they need
 * be Comparable. Regions are ordered based on their start offset.
 */
class Region implements Comparable<Region> {
    // The location and length of the Region
    private String filename;
    private long offset;
    private long length;

    // The buffer where this Region's data is prefetched
    private MiniBuffer buffer;

    // The offset into `buffer` where this Region's data starts. i.e. index
    // `bufferOffset` into `buffer` corresponds with offset `offset` into the
    // given file.
    private long bufferOffset;

    /**
     * Create a new Region with the given location, length, buffer, and
     * bufferOffset.
     */
    public Region(String filename,
                  long offset,
                  long length,
                  MiniBuffer buffer,
                  long bufferOffset)
    {
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
    }

    /**
     * Create a new Region with the given location, length, buffer, and
     * bufferOffset.
     */
    public Region(String filename,
                  long offset,
                  long length)
    {
        this.filename = filename;
        this.offset = offset;
        this.length = length;

        // Will be set later
        this.buffer = null;
        this.bufferOffset = 0;
    }

    /**
     * @return the offset into its file of this Region.
     */
    public long getStart() {
        return this.offset;
    }

    /**
     * @return the offset into its file of the first byte after this Region.
     */
    public long getAfter() {
        return this.offset + this.length;
    }

    /**
     * Set the buffer and bufferOffset of this region.
     */
    public void setBuffer(MiniBuffer buffer, long bufferOffset) {
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
    }

    /**
     * Compare two Regions.
     *
     * @throws IllegalArgumentException if they are not from the same file.
     */
    public int compareTo(Region r) {
        if (!this.filename.equals(r.filename)) {
            throw new IllegalArgumentException(
                    "Cannot compare regions of different files");
        }

        if (this.offset < r.offset) {
            return -1;
        } else if (this.offset == r.offset) {
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Split this region into two at the given offset (into the file).
     *
     * This object will retain the portion before the offset, while the
     * returned object will contain the remainder of the region.
     *
     * @param offset the offset into the file where this region is to be split.
     * @return a new Region with the portion of this Region after offset.
     * @throws IllegalArgumentException if offset is not in this region.
     */
    public Region splitAt(long offset) {
        // Sanity
        if (offset >= this.offset + this.length) {
            throw new IllegalArgumentException(
                    "The requested offset (" + offset
                    +") is beyond the end of this Region (" + this + ")");
        }

        // Create a new Region (the portion after `offset`)
        long newLengthPost = offset - this.offset; // length of part before split
        long newLengthPre = this.length - newLengthPost; // and after split
        Region r = new Region(
                this.filename,
                offset,
                newLengthPost,
                this.buffer,
                this.bufferOffset + newLengthPre);

        // Update this region
        this.length = newLengthPre;

        return r;
    }

    public String toString() {
        return "Region { file=" + this.filename +
            ", offset=" + this.offset +
            ", length=" + this.length + " }";
    }
}
